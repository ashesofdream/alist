package quark

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/alist-org/alist/v3/drivers/base"
	"github.com/alist-org/alist/v3/internal/conf"
	"github.com/alist-org/alist/v3/internal/driver"
	"github.com/alist-org/alist/v3/internal/errs"
	"github.com/alist-org/alist/v3/internal/model"
	"github.com/alist-org/alist/v3/pkg/utils"
	"github.com/go-resty/resty/v2"
	log "github.com/sirupsen/logrus"
)

type QuarkOrUC struct {
	model.Storage
	Addition
	config driver.Config
	conf   Conf
}

func (d *QuarkOrUC) Config() driver.Config {
	return d.config
}

func (d *QuarkOrUC) GetAddition() driver.Additional {
	return &d.Addition
}

func (d *QuarkOrUC) Init(ctx context.Context) error {
	_, err := d.request("/config", http.MethodGet, nil, nil)
	return err
}

func (d *QuarkOrUC) Drop(ctx context.Context) error {
	return nil
}

func (d *QuarkOrUC) List(ctx context.Context, dir model.Obj, args model.ListArgs) ([]model.Obj, error) {
	files, err := d.GetFiles(dir.GetID())
	if err != nil {
		return nil, err
	}
	return utils.SliceConvert(files, func(src File) (model.Obj, error) {
		return fileToObj(src), nil
	})
}

func (d *QuarkOrUC) Link(ctx context.Context, file model.Obj, args model.LinkArgs) (*model.Link, error) {
	data := base.Json{
		"fids": []string{file.GetID()},
	}
	var resp DownResp
	ua := d.conf.ua
	_, err := d.request("/file/download", http.MethodPost, func(req *resty.Request) {
		req.SetHeader("User-Agent", ua).
			SetBody(data)
	}, &resp)
	if err != nil {
		return nil, err
	}

	return &model.Link{
		URL: resp.Data[0].DownloadUrl,
		Header: http.Header{
			"Cookie":     []string{d.Cookie},
			"Referer":    []string{d.conf.referer},
			"User-Agent": []string{ua},
		},
		Concurrency: 2,
		PartSize:    10 * utils.MB,
	}, nil
}

func (d *QuarkOrUC) MakeDir(ctx context.Context, parentDir model.Obj, dirName string) error {
	data := base.Json{
		"dir_init_lock": false,
		"dir_path":      "",
		"file_name":     dirName,
		"pdir_fid":      parentDir.GetID(),
	}
	_, err := d.request("/file", http.MethodPost, func(req *resty.Request) {
		req.SetBody(data)
	}, nil)
	if err == nil {
		time.Sleep(time.Second)
	}
	return err
}

func (d *QuarkOrUC) Move(ctx context.Context, srcObj, dstDir model.Obj) error {
	data := base.Json{
		"action_type":  1,
		"exclude_fids": []string{},
		"filelist":     []string{srcObj.GetID()},
		"to_pdir_fid":  dstDir.GetID(),
	}
	_, err := d.request("/file/move", http.MethodPost, func(req *resty.Request) {
		req.SetBody(data)
	}, nil)
	return err
}

func (d *QuarkOrUC) Rename(ctx context.Context, srcObj model.Obj, newName string) error {
	data := base.Json{
		"fid":       srcObj.GetID(),
		"file_name": newName,
	}
	_, err := d.request("/file/rename", http.MethodPost, func(req *resty.Request) {
		req.SetBody(data)
	}, nil)
	return err
}

func (d *QuarkOrUC) Copy(ctx context.Context, srcObj, dstDir model.Obj) error {
	return errs.NotSupport
}

func (d *QuarkOrUC) Remove(ctx context.Context, obj model.Obj) error {
	data := base.Json{
		"action_type":  1,
		"exclude_fids": []string{},
		"filelist":     []string{obj.GetID()},
	}
	_, err := d.request("/file/delete", http.MethodPost, func(req *resty.Request) {
		req.SetBody(data)
	}, nil)
	return err
}

func (d *QuarkOrUC) Put(ctx context.Context, dstDir model.Obj, stream model.FileStreamer, up driver.UpdateProgress) error {
	log.Infof("quark upload %s:cache fill to temp file", stream.GetName())
	tempFile, err := stream.CacheFullInTempFile()
	if err != nil {
		return err
	}
	defer func() {
		_ = tempFile.Close()
	}()
	log.Infof("quark upload %s:calculate md5", stream.GetName())
	m := md5.New()
	_, err = utils.CopyWithBuffer(m, tempFile)
	if err != nil {
		return err
	}
	_, err = tempFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	md5Str := hex.EncodeToString(m.Sum(nil))
	log.Infof("quark upload %s:calculate sha1", stream.GetName())
	s := sha1.New()
	_, err = utils.CopyWithBuffer(s, tempFile)
	if err != nil {
		return err
	}
	_, err = tempFile.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	sha1Str := hex.EncodeToString(s.Sum(nil))
	log.Infof("quark upload %s:pre", stream.GetName())
	// pre
	pre, err := d.upPre(stream, dstDir.GetID())
	if err != nil {
		return err
	}
	log.Debugln("hash: ", md5Str, sha1Str)
	// hash
	finish, err := d.upHash(md5Str, sha1Str, pre.Data.TaskId)
	if err != nil {
		return err
	}
	if finish {
		return nil
	}
	//part worker start
	log.Infof("upload file %s  part size: %d,part thread: %d", stream.GetName(), pre.Metadata.PartSize, pre.Metadata.PartThread)
	partDataChannel := make(chan *PartData, pre.Metadata.PartThread)
	partReturnChannel := make(chan *PartReturn, pre.Metadata.PartThread)
	wg := sync.WaitGroup{}
	md5s := make([]string, stream.GetSize()/int64(pre.Metadata.PartSize)+1)
	if stream.GetSize()%int64(pre.Metadata.PartSize) == 0 {
		md5s = md5s[:len(md5s)-1]
	}
	for i := 0; i < pre.Metadata.PartThread; i++ {
		wg.Add(1)
		go d.PutWokrer(stream.GetName()+" upload worker_"+strconv.Itoa(i), partDataChannel, partReturnChannel, &wg, ctx, &pre, stream)
	}
	//gather worker

	var gatherError error
	gatherContext, gatherCancel := context.WithCancel(ctx)
	go func() {
		log.Infof("gather worker start")
		defer log.Infof("gather worker end")
		total := stream.GetSize()
		left := total
		var partReturn *PartReturn
		for true {
			select {
			case rtn := <-partReturnChannel:
				partReturn = rtn
			case <-gatherContext.Done():
				return
			}
			if partReturn.err != nil {
				// if error, stop all workers
				gatherError = partReturn.err
				return
			}
			if partReturn.md5 == "" {
				gatherError = fmt.Errorf("part %d md5 is empty", partReturn.partNum)
				return
			}
			left -= int64(partReturn.partSize)
			md5s[partReturn.partNum-1] = partReturn.md5
			up(100 * float64(total-left) / float64(total))
			log.Infof("gather: %d part finished, left size: %d", partReturn.partNum, left)
			if left <= 0 {
				return
			}
		}
	}()
	// part up
	partSize := pre.Metadata.PartSize
	var bytes []byte
	total := stream.GetSize()
	left := total
	partNumber := 1
	shaHasher := NewSHA1()
	defer gatherCancel()
	for left > 0 {
		if utils.IsCanceled(ctx) {
			return ctx.Err()
		}
		if left > int64(partSize) {
			bytes = make([]byte, partSize)
		} else {
			bytes = make([]byte, left)
		}
		_, err := io.ReadFull(tempFile, bytes)
		if err != nil {
			return err
		}
		left -= int64(len(bytes))
		log.Debugf("left: %d", left)
		// get last state before write
		h, offset := shaHasher.GetState()
		nl, nh := offset2nlnh(offset)
		if _, err = shaHasher.Write(bytes); err != nil {
			return err
		}

		select {
		case partDataChannel <- &PartData{data: bytes, partNum: partNumber, hashContext: OssHashContext{
			HashType: "sha1",
			H0:       strconv.FormatUint(uint64(h[0]), 10),
			H1:       strconv.FormatUint(uint64(h[1]), 10),
			H2:       strconv.FormatUint(uint64(h[2]), 10),
			H3:       strconv.FormatUint(uint64(h[3]), 10),
			H4:       strconv.FormatUint(uint64(h[4]), 10),
			Nl:       strconv.FormatUint(uint64(nl), 10),
			Nh:       strconv.FormatUint(uint64(nh), 10),
			Data:     "",
			Num:      "0",
		}}:
		case <-ctx.Done():
			close(partDataChannel)
			return gatherError
		}
		partNumber++
	}
	close(partDataChannel)
	wg.Wait()
	close(partReturnChannel)
	log.Infof("file %s gather finish, error: %v", stream.GetName(), gatherError)
	if gatherError != nil {
		return gatherError
	}
	err = d.upCommit(pre, md5s)
	if err != nil {
		return err
	}
	return d.upFinish(pre)
}

type PartData struct {
	data        []byte
	partNum     int
	hashContext OssHashContext //"X-Oss-Hash-Ctx"
}

type PartReturn struct {
	partNum  int
	partSize int
	md5      string
	err      error
}

func (d *QuarkOrUC) PutWokrer(workerName string, dataChannels <-chan *PartData, finishChannel chan<- *PartReturn, wgGp *sync.WaitGroup,
	ctx context.Context, pre *UpPreResp, stream model.FileStreamer) error {
	defer wgGp.Done()
	defer log.Infof("%s finish", workerName)
	log.Infof("%s start", workerName)
	for partData := range dataChannels {
		if utils.IsCanceled(ctx) {
			return ctx.Err()
		}
		var m string
		var err error
		log.Infof("%s put part %d,size:%d", workerName, partData.partNum, len(partData.data))
		for i := 0; conf.Conf.Tasks.Copy.MaxRetry*2 > i; i++ {
			m, err = d.upPart(ctx, *pre, stream.GetMimetype(), partData.partNum, partData.data, &partData.hashContext)
			if err == nil {
				break
			}
			log.Errorf("%s : quark upload file %s  part %d error: %s, retry %d", workerName, stream.GetName(), partData.partNum, err, i+1)
			time.Sleep(time.Second * 15)
		}
		//m, err := driver.UpPart(pre, file.GetMIMEType(), partNumber, bytes, account, md5Str, sha1Str)
		// if err != nil {
		// 	return err
		// }
		if m == "finish" {
			return nil
		}
		finishChannel <- &PartReturn{partNum: partData.partNum, partSize: len(partData.data), err: err, md5: m}
	}
	return nil
}

var _ driver.Driver = (*QuarkOrUC)(nil)
