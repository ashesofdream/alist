package proxycache

import (
	"regexp"
)

var compiledPattern *regexp.Regexp

// var imgExt = map[string]bool{
// 	".png":  true,
// 	".jpg":  true,
// 	".webp": true,
// 	".jpeg": true,
// }

// func IsImgFile(path string) bool {
// 	ext := strings.ToLower(stdpath.Ext(path))
// 	if _, found := imgExt[ext]; found {
// 		return true
// 	}
// 	return false
// }

func initRegexp(pattern string) error {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}
	compiledPattern = re
	return nil
}

func isMatch(path string) bool {
	return compiledPattern.MatchString(path)
}
