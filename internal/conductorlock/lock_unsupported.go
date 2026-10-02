//go:build !linux && !darwin

package conductorlock

import "os"

func openAndLock(string) (*os.File, error) { return nil, &Error{Kind: ErrIO} }

func ownedByCurrentUser(os.FileInfo) bool { return false }
