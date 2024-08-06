package proxycache

import (
	"fmt"
	"io"
	"net/http"

	"github.com/alist-org/alist/v3/internal/driver"
	"github.com/alist-org/alist/v3/internal/model"
	"github.com/alist-org/alist/v3/pkg/http_range"
	"github.com/ashesofdream/go-disklrucache"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type unfinishedInfo struct {
	editor       *disklrucache.DiskLRUCacheEditor
	writer       *disklrucache.EditorWriter
	nextWritePos int64 // next write position for range request
}

var unfinishCache map[string]*unfinishedInfo

type HttpResponseWriterWarpper struct {
	http.ResponseWriter
	writer     io.Writer
	closeFunc  func() error
	writePos   int64
	writeError error
}

func (w *HttpResponseWriterWarpper) Write(p []byte) (int, error) {
	cnt, err := w.writer.Write(p)
	w.writePos += int64(cnt)
	if err != nil {
		w.writeError = err
	}
	return cnt, err
}

func (w *HttpResponseWriterWarpper) Close() error {
	return w.closeFunc()
}

/*
this function will writer a record to database
and return a http multiWriter that write to both responseWriter and local file
*/
func _WrapHttpWriter(w http.ResponseWriter, r *http.Request, storage driver.Driver, file model.Obj) (*HttpResponseWriterWarpper, error) {
	name := digest(storage, file)
	info, ok := unfinishCache[name]
	var writer *disklrucache.EditorWriter = nil
	var editor *disklrucache.DiskLRUCacheEditor = nil
	var nextWritePos = int64(0)
	if ok {
		editor = info.editor
		writer = info.writer
		nextWritePos = info.nextWritePos
	} else {
		editor = cache.Edit(name)
	}
	if editor == nil {
		panic("cache edit failed")
	}
	if writer == nil {
		_writer, err := editor.CreateRandomWriter()
		if err != nil {
			return nil, errors.Wrap(err, "create output stream failed")
		}
		writer = _writer
	}
	//range request need to cache
	range_header := r.Header.Get("Range")
	if range_header != "" {
		ranges, err := http_range.ParseRange(range_header, file.GetSize())
		fmt.Printf("ranges raw:%s ,start:%d,end:%d\n", range_header, ranges[0].Start, ranges[0].Length+ranges[0].Start)
		if len(ranges) != 1 || ranges[0].Start > nextWritePos {
			//only support one range now and not sparse cache
			if len(ranges) == 1 {
				fmt.Printf("range cache fail,last write end pos:%d, range start:%d\n", nextWritePos, ranges[0].Start)
				cache.Remove(name)
				delete(unfinishCache, name)
			}
			return _WrapHttpWriterSimple(w), nil
		}

		if err != nil {
			return nil, errors.Wrap(err, "parse range failed")
		}
		writer.Seek(ranges[0].Start, io.SeekStart)
	}
	wrapper := &HttpResponseWriterWarpper{
		ResponseWriter: w,
		writer:         io.MultiWriter(w, writer),
	}
	wrapper.closeFunc = func() error {
		trueFileSize := file.GetSize()
		if wrapper.writeError == io.ErrClosedPipe || editor.FileSize() > trueFileSize {
			//cache error
			writer.Close()
			cache.Remove(name)
			delete(unfinishCache, name)
			fmt.Printf("cache write error ,reason :%s", wrapper.writeError)
		} else if editor.FileSize() == trueFileSize {
			writer.Close()
			if err := editor.Commit(); err != nil {
				return errors.Wrap(err, "diskcache commit failed")
			}
			delete(unfinishCache, name)
			log.Infof("write cache successfully,rest unfinished size:%d", len(unfinishCache))
			return nil
		} else {
			// cache editor that hash not finish
			fmt.Printf("have writeen %d\n", editor.FileSize())
			unfinishCache[name] = &unfinishedInfo{editor, writer, nextWritePos + wrapper.writePos}

		}
		return nil
	}
	return wrapper, nil
}
func _WrapHttpWriterSimple(w http.ResponseWriter) *HttpResponseWriterWarpper {
	return &HttpResponseWriterWarpper{
		w,
		w,
		func() error {
			return nil
		},
		0,
		nil,
	}
}

func isEditorNotFinish(name string) bool {
	_, ok := unfinishCache[name]
	return ok
}
