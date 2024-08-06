package proxycache

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"time"

	"github.com/alist-org/alist/v3/internal/conf"
	"github.com/alist-org/alist/v3/internal/driver"
	"github.com/alist-org/alist/v3/internal/model"
	"github.com/ashesofdream/go-disklrucache"
	log "github.com/sirupsen/logrus"
)

var isExpierd func(CreateTime time.Time, file model.Obj) bool
var cache *disklrucache.DiskLRUCache = nil

func Init(config *conf.ProxyCache) {
	if err := initRegexp(config.MatchPattern); err != nil {
		log.Warn("invalid match pattern, disable cacheproxy")
		return
	}
	if config.FileDiffStrategy == "UpdateWhenModified" {
		isExpierd = func(createTime time.Time, file model.Obj) bool {
			return file.ModTime().After(createTime)
		}
	} else {
		log.Warn("unknow cacheproxy strategy,disable")
		return
	}
	cache = disklrucache.CreateDiskLRUCache(config.Path, 1, 1, config.MaxSize)
	unfinishCache = make(map[string]*unfinishedInfo)
}

func digest(storage driver.Driver, file model.Obj) string {
	all_path := storage.Config().Name + file.GetPath()
	hash := sha256.Sum256([]byte(all_path))
	return hex.EncodeToString(hash[:])
}

func GetCacheFile(storage driver.Driver, file model.Obj, link *model.Link) (model.File, error) {
	if cache == nil {
		return nil, nil
	}
	cache_name := digest(storage, file)
	if isEditorNotFinish(cache_name) {
		return nil, nil
	}

	snapshot, err := cache.Get(cache_name)
	if snapshot == nil || err != nil {
		return nil, err
	}
	if snapshot.Size != file.GetSize() || isExpierd(snapshot.Time, file) {
		//just return nil , let wrap http writer update cache
		log.Debug("Cache Out of date,file:%s", file.GetPath())
		return nil, nil
	}

	fmt.Printf("Cache hit %s\n", file.GetPath())

	return snapshot.Reader, nil
}

func WrapHttpWriter(w http.ResponseWriter, r *http.Request, storage driver.Driver, file model.Obj, link *model.Link) (*HttpResponseWriterWarpper, error) {
	if cache == nil || !isMatch(file.GetName()) || link.MFile != nil {
		return _WrapHttpWriterSimple(w), nil
	}
	return _WrapHttpWriter(w, r, storage, file)
}

func RemoveCache(storage driver.Driver, file model.Obj) {
	cache_name := digest(storage, file)
	cache.Remove(cache_name)
}
