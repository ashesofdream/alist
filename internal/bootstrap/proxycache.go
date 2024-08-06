package bootstrap

import (
	"github.com/alist-org/alist/v3/internal/conf"
	"github.com/alist-org/alist/v3/internal/proxycache"
)

func InitProxyCache() {
	// os.RemoveAll(conf.Conf.ProxyCache.Path)
	proxycache.Init(&conf.Conf.ProxyCache)

}
