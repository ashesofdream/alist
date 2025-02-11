package quark

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/alist-org/alist/v3/drivers/base"
	"github.com/alist-org/alist/v3/internal/model"
	"github.com/alist-org/alist/v3/internal/op"
	"github.com/alist-org/alist/v3/pkg/cookie"
	"github.com/alist-org/alist/v3/pkg/utils"
	"github.com/go-resty/resty/v2"
	log "github.com/sirupsen/logrus"
)

// do others that not defined in Driver interface
var uploadClient *resty.Client

type ZerobyteTimeoutReader struct {
	io.Reader
	timeoutCtx    context.Context
	ctxCancelFunc context.CancelFunc
	timeout       time.Duration
	ticker        *time.Ticker
	lock          sync.Mutex
}

func NewZerobyteTimeoutReader(reader io.Reader, timeout time.Duration, ctx context.Context) *ZerobyteTimeoutReader {
	warp_reader := &ZerobyteTimeoutReader{Reader: reader, timeout: timeout}
	warp_reader.timeoutCtx, warp_reader.ctxCancelFunc = context.WithCancel(ctx)
	return warp_reader
}

func (r *ZerobyteTimeoutReader) Read(p []byte) (n int, err error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if utils.IsCanceled(r.timeoutCtx) {
		return 0, io.EOF
	}
	n, err = r.Reader.Read(p)
	log.Infof("zerobytereader read %d bytes, err %v", n, err)
	r.ticker.Reset(r.timeout)
	return n, err
}

func (r *ZerobyteTimeoutReader) Watch() {
	r.ticker = time.NewTicker(r.timeout)
	log.Infof("zerobytereader watch routine start")
	go func() {
		select {
		case <-r.timeoutCtx.Done():
		case <-r.ticker.C:
			r.lock.Lock()
			defer r.lock.Unlock()
			r.ctxCancelFunc()
			r.ticker.Stop()
			log.Info("zerobytereader watch routine end")
			return
		}
	}()
}
func (r *ZerobyteTimeoutReader) Ctx() context.Context {
	return r.timeoutCtx
}

func init() {
	uploadClient = resty.New().
		SetHeader("user-agent", base.UserAgent).
		SetRetryCount(3).
		SetRetryResetReaders(true).
		SetTimeout(5 * time.Minute)
}
func (d *QuarkOrUC) uploadRequest(pathname string, method string, callback base.ReqCallback, resp interface{}) ([]byte, error) {
	u := d.conf.api + pathname
	req := uploadClient.R()
	req.SetHeaders(map[string]string{
		"Cookie":  d.Cookie,
		"Accept":  "application/json, text/plain, */*",
		"Referer": d.conf.referer,
	})
	req.SetQueryParam("pr", d.conf.pr)
	req.SetQueryParam("fr", "pc")
	if callback != nil {
		callback(req)
	}
	if resp != nil {
		req.SetResult(resp)
	}
	var e Resp
	req.SetError(&e)
	res, err := req.Execute(method, u)
	if err != nil {
		return nil, err
	}
	__puus := cookie.GetCookie(res.Cookies(), "__puus")
	if __puus != nil {
		d.Cookie = cookie.SetStr(d.Cookie, "__puus", __puus.Value)
		op.MustSaveDriverStorage(d)
	}
	if e.Status >= 400 || e.Code != 0 {
		return nil, errors.New(e.Message)
	}
	return res.Body(), nil
}

func (d *QuarkOrUC) request(pathname string, method string, callback base.ReqCallback, resp interface{}) ([]byte, error) {
	u := d.conf.api + pathname
	req := uploadClient.R()
	req.SetHeaders(map[string]string{
		"Cookie":  d.Cookie,
		"Accept":  "application/json, text/plain, */*",
		"Referer": d.conf.referer,
	})
	req.SetQueryParam("pr", d.conf.pr)
	req.SetQueryParam("fr", "pc")
	if callback != nil {
		callback(req)
	}
	if resp != nil {
		req.SetResult(resp)
	}
	var e Resp
	req.SetError(&e)
	res, err := req.Execute(method, u)
	if err != nil {
		return nil, err
	}
	__puus := cookie.GetCookie(res.Cookies(), "__puus")
	if __puus != nil {
		d.Cookie = cookie.SetStr(d.Cookie, "__puus", __puus.Value)
		op.MustSaveDriverStorage(d)
	}
	if e.Status >= 400 || e.Code != 0 {
		return nil, errors.New(e.Message)
	}
	return res.Body(), nil
}

func (d *QuarkOrUC) GetFiles(parent string) ([]File, error) {
	files := make([]File, 0)
	page := 1
	size := 100
	query := map[string]string{
		"pdir_fid":     parent,
		"_size":        strconv.Itoa(size),
		"_fetch_total": "1",
	}
	if d.OrderBy != "none" {
		query["_sort"] = "file_type:asc," + d.OrderBy + ":" + d.OrderDirection
	}
	for {
		query["_page"] = strconv.Itoa(page)
		var resp SortResp
		_, err := d.request("/file/sort", http.MethodGet, func(req *resty.Request) {
			req.SetQueryParams(query)
		}, &resp)
		if err != nil {
			return nil, err
		}
		files = append(files, resp.Data.List...)
		if page*size >= resp.Metadata.Total {
			break
		}
		page++
	}
	return files, nil
}

func (d *QuarkOrUC) upPre(file model.FileStreamer, parentId string) (UpPreResp, error) {
	now := time.Now()
	data := base.Json{
		"ccp_hash_update": true,
		"dir_name":        "",
		"file_name":       file.GetName(),
		"format_type":     file.GetMimetype(),
		"l_created_at":    now.UnixMilli(),
		"l_updated_at":    now.UnixMilli(),
		"pdir_fid":        parentId,
		"size":            file.GetSize(),
		"parallel_upload": true,
		//"same_path_reuse": true,
	}
	var resp UpPreResp
	_, err := d.request("/file/upload/pre", http.MethodPost, func(req *resty.Request) {
		req.SetBody(data)
	}, &resp)
	return resp, err
}

func (d *QuarkOrUC) upHash(md5, sha1, taskId string) (bool, error) {
	data := base.Json{
		"md5":     md5,
		"sha1":    sha1,
		"task_id": taskId,
	}
	log.Debugf("hash: %+v", data)
	var resp HashResp
	_, err := d.request("/file/update/hash", http.MethodPost, func(req *resty.Request) {
		req.SetBody(data)
	}, &resp)
	return resp.Data.Finish, err
}

func (d *QuarkOrUC) upPart(ctx context.Context, pre UpPreResp, mineType string, partNumber int, dataBytes []byte, hashCtx *OssHashContext) (string, error) {
	//func (driver QuarkOrUC) UpPart(pre UpPreResp, mineType string, partNumber int, bytes []byte, account *model.Account, md5Str, sha1Str string) (string, error) {
	timeStr := time.Now().UTC().Format(http.TimeFormat)
	hash := md5.Sum(dataBytes)
	hashHex := hex.EncodeToString(hash[:])
	ossHashCtxByte, err := json.Marshal(hashCtx)
	if err != nil {
		return "", err
	}
	ossHashBase64 := base64.StdEncoding.EncodeToString(ossHashCtxByte)
	var ossHashCtxHeaderLine string = ""
	if partNumber > 1 {
		ossHashCtxHeaderLine = fmt.Sprintf("x-oss-hash-ctx:%s\n", ossHashBase64)
	}
	data := base.Json{
		"auth_info": pre.Data.AuthInfo,
		"auth_meta": fmt.Sprintf(`PUT

%s
%s
x-oss-date:%s
x-oss-user-agent:aliyun-sdk-js/6.6.1 Chrome 98.0.4758.80 on Windows 10 64-bit
%s
/%s/%s?partNumber=%d&uploadId=%s`,
			mineType, timeStr, timeStr, ossHashCtxHeaderLine, pre.Data.Bucket, pre.Data.ObjKey, partNumber, pre.Data.UploadId),
		"task_id": pre.Data.TaskId,
	}
	var resp UpAuthResp
	_, err = d.request("/file/upload/auth", http.MethodPost, func(req *resty.Request) {
		req.SetBody(data).SetContext(ctx)
	}, &resp)
	if err != nil {
		return "", err
	}

	header := map[string]string{
		"Authorization":    resp.Data.AuthKey,
		"Content-Type":     mineType,
		"Referer":          "https://pan.quark.cn/",
		"x-oss-date":       timeStr,
		"x-oss-user-agent": "aliyun-sdk-js/6.6.1 Chrome 98.0.4758.80 on Windows 10 64-bit",
	}
	if partNumber > 1 {
		header["x-oss-hash-ctx"] = ossHashBase64
	}
	log.Infof("partNumber: %d, ctx: %s", partNumber, ossHashBase64)
	u := fmt.Sprintf("https://%s.%s/%s", pre.Data.Bucket, pre.Data.UploadUrl[7:], pre.Data.ObjKey)
	res, err := uploadClient.R().SetContext(ctx).
		SetHeaders(header).
		SetQueryParams(map[string]string{
			"partNumber": strconv.Itoa(partNumber),
			"uploadId":   pre.Data.UploadId,
		}).SetBody(dataBytes).Put(u)
	if res.StatusCode() == 409 {
		var result = OssErrorBody{}
		err := xml.Unmarshal(res.Body(), &result)
		if err != nil {
			return "", fmt.Errorf("up status: %d, error: %s,unmarshal xml error: %s", res.StatusCode(), res.String(), err.Error())
		}
		if result.Code == "PartAlreadyExist" && result.PartEtag == hashHex {
			log.Debugf("part already exist, skip,Etag: %s", result.PartEtag)
			return result.PartEtag, nil
		}
	}
	if res.StatusCode() != 200 {
		return "", fmt.Errorf("up status: %d,Remote Etag: %s, True Etag: %s, error: %s", res.StatusCode(), res.Header().Get("ETag"), hashHex, res.String())
	}
	return res.Header().Get("ETag"), nil
}

func (d *QuarkOrUC) upCommit(pre UpPreResp, md5s []string) error {
	timeStr := time.Now().UTC().Format(http.TimeFormat)
	log.Debugf("md5s: %+v", md5s)
	bodyBuilder := strings.Builder{}
	bodyBuilder.WriteString(`<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUpload>
`)
	for i, m := range md5s {
		bodyBuilder.WriteString(fmt.Sprintf(`<Part>
<PartNumber>%d</PartNumber>
<ETag>%s</ETag>
</Part>
`, i+1, m))
	}
	bodyBuilder.WriteString("</CompleteMultipartUpload>")
	body := bodyBuilder.String()
	log.Infof("commit body: %s", body)
	m := md5.New()
	m.Write([]byte(body))
	contentMd5 := base64.StdEncoding.EncodeToString(m.Sum(nil))
	callbackBytes, err := utils.Json.Marshal(pre.Data.Callback)
	if err != nil {
		return err
	}
	callbackBase64 := base64.StdEncoding.EncodeToString(callbackBytes)
	data := base.Json{
		"auth_info": pre.Data.AuthInfo,
		"auth_meta": fmt.Sprintf(`POST
%s
application/xml
%s
x-oss-callback:%s
x-oss-date:%s
x-oss-user-agent:aliyun-sdk-js/6.6.1 Chrome 98.0.4758.80 on Windows 10 64-bit
/%s/%s?uploadId=%s`,
			contentMd5, timeStr, callbackBase64, timeStr,
			pre.Data.Bucket, pre.Data.ObjKey, pre.Data.UploadId),
		"task_id": pre.Data.TaskId,
	}
	log.Debugf("xml: %s", body)
	log.Debugf("auth data: %+v", data)
	var resp UpAuthResp
	_, err = d.request("/file/upload/auth", http.MethodPost, func(req *resty.Request) {
		req.SetBody(data)
	}, &resp)
	if err != nil {
		return err
	}
	u := fmt.Sprintf("https://%s.%s/%s", pre.Data.Bucket, pre.Data.UploadUrl[7:], pre.Data.ObjKey)
	res, err := uploadClient.R().
		SetHeaders(map[string]string{
			"Authorization":    resp.Data.AuthKey,
			"Content-MD5":      contentMd5,
			"Content-Type":     "application/xml",
			"Referer":          "https://pan.quark.cn/",
			"x-oss-callback":   callbackBase64,
			"x-oss-date":       timeStr,
			"x-oss-user-agent": "aliyun-sdk-js/6.6.1 Chrome 98.0.4758.80 on Windows 10 64-bit",
		}).
		SetQueryParams(map[string]string{
			"uploadId": pre.Data.UploadId,
		}).SetBody(body).Post(u)
	if res.StatusCode() != 200 {
		return fmt.Errorf("up status: %d, error: %s", res.StatusCode(), res.String())
	}
	return nil
}

func (d *QuarkOrUC) upFinish(pre UpPreResp) error {
	data := base.Json{
		"obj_key": pre.Data.ObjKey,
		"task_id": pre.Data.TaskId,
	}
	_, err := d.request("/file/upload/finish", http.MethodPost, func(req *resty.Request) {
		req.SetBody(data)
	}, nil)
	if err != nil {
		return err
	}
	time.Sleep(time.Second)
	return nil
}
