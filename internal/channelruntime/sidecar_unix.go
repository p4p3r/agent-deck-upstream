//go:build linux || darwin

package channelruntime

import (
	"os"
	"syscall"
)

func ownedSingleLink(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Geteuid()) && stat.Nlink == 1
}
