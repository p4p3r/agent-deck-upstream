//go:build linux

package main

func runningBinarySHA256() (string, error) {
	return sha256File("/proc/self/exe")
}
