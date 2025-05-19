package quark

import (
	"bytes"
	"context"
	"encoding/hex"
	"hash"
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
	streamPkg "github.com/alist-org/alist/v3/internal/stream"
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
		Concurrency: 3,
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
	md5Str, sha1Str := stream.GetHash().GetHash(utils.MD5), stream.GetHash().GetHash(utils.SHA1)
	var (
		md5  hash.Hash
		sha1 hash.Hash
	)
	writers := []io.Writer{}
	if len(md5Str) != utils.MD5.Width {
		md5 = utils.MD5.NewFunc()
		writers = append(writers, md5)
	}
	if len(sha1Str) != utils.SHA1.Width {
		sha1 = utils.SHA1.NewFunc()
		writers = append(writers, sha1)
	}

	if len(writers) > 0 {
		_, err := streamPkg.CacheFullInTempFileAndWriter(stream, io.MultiWriter(writers...))
		if err != nil {
			return err
		}
		if md5 != nil {
			md5Str = hex.EncodeToString(md5.Sum(nil))
		}
		if sha1 != nil {
			sha1Str = hex.EncodeToString(sha1.Sum(nil))
		}
	}
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
	max_thread := max(pre.Metadata.PartThread, 2)
	for i := 0; i < max_thread; i++ {
		wg.Add(1)
		go d.PutWokrer(stream.GetName()+" upload worker_"+strconv.Itoa(i), partDataChannel, partReturnChannel, &wg, ctx, &pre, stream)
	}
	//gather worker

	var gatherError error
	gatherContext, gatherCancel := context.WithCancel(ctx)
	gatherWg := sync.WaitGroup{}
	gatherWg.Add(1)
	go func() {
		log.Infof("gather worker start")
		defer log.Infof("gather worker end")
		total := stream.GetSize()
		left := total
		var partReturn *PartReturn
		defer gatherWg.Done()
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
	total := stream.GetSize()
	left := total
	partSize := int64(pre.Metadata.PartSize)
	part := make([]byte, partSize)
	count := int(total / partSize)
	if total%partSize > 0 {
		count++
	}
	md5s := make([]string, 0, count)
	partNumber := 1
	shaHasher := NewSHA1()
	defer gatherCancel()
	for left > 0 {
		if utils.IsCanceled(ctx) {
			return ctx.Err()
		}
		if left < partSize {
			part = part[:left]
		}
		n, err := io.ReadFull(stream, part)
		if err != nil {
			gatherError = err
			break
		}
		left -= int64(n)
		log.Debugf("left: %d", left)
		reader := driver.NewLimitedUploadStream(ctx, bytes.NewReader(part))
		m, err := d.upPart(ctx, pre, stream.GetMimetype(), partNumber, reader)
		//m, err := driver.UpPart(pre, file.GetMIMEType(), partNumber, bytes, account, md5Str, sha1Str)
		if err != nil {
			return err
		}
		if m == "finish" {
			return nil
		}
		md5s = append(md5s, m)
		partNumber++
	}
	close(partDataChannel)
	wg.Wait()
	close(partReturnChannel)
	gatherWg.Wait()
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
