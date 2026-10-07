package slackcontrol

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"
)

var (
	ErrUnsafePath  = errors.New("slackcontrol: unsafe socket path")
	ErrInUse       = errors.New("slackcontrol: socket in use")
	ErrUnavailable = errors.New("slackcontrol: unavailable")
)

type Config struct {
	Path          string
	UID           int
	IOTimeout     time.Duration
	StatusTimeout time.Duration
	Snapshot      func(context.Context) (Status, error)
	PeerUID       func(*net.UnixConn) (int, error)
}

type Completion struct {
	RunnerState string
	Recoverable bool
}

type socketIdentity struct {
	device uint64
	inode  uint64
}

type Server struct {
	listener   *net.UnixListener
	path       string
	uid        int
	identity   socketIdentity
	lock       *os.File
	config     Config
	completed  chan Completion
	done       chan error
	statusGate chan struct{}
	closeOnce  sync.Once
	wg         sync.WaitGroup
}

func Start(config Config) (*Server, error) {
	if config.IOTimeout == 0 {
		config.IOTimeout = 500 * time.Millisecond
	}
	if config.StatusTimeout == 0 {
		config.StatusTimeout = 500 * time.Millisecond
	}
	if config.UID != os.Getuid() || config.Snapshot == nil || config.IOTimeout < time.Millisecond ||
		config.StatusTimeout < time.Millisecond || config.StatusTimeout > config.IOTimeout {
		return nil, ErrUnavailable
	}
	if config.PeerUID == nil {
		config.PeerUID = peerUID
	}
	if err := validateSocketParent(config.Path, config.UID); err != nil {
		return nil, err
	}
	lock, err := acquireSocketLock(config.Path+".lock", config.UID)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*Server, error) {
		_ = unlockAndClose(lock)
		return nil, err
	}
	if err := clearStaleSocket(config.Path, config.UID); err != nil {
		return fail(err)
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: config.Path, Net: "unix"})
	if err != nil {
		return fail(ErrUnavailable)
	}
	listener.SetUnlinkOnClose(false)
	created, err := inspectOwnedSocket(config.Path, config.UID, false)
	if err != nil {
		_ = listener.Close()
		return fail(err)
	}
	if err := os.Chmod(config.Path, 0o600); err != nil {
		_ = listener.Close()
		_ = removeOwnedSocket(config.Path, config.UID, created)
		return fail(ErrUnavailable)
	}
	identity, err := inspectSocket(config.Path, config.UID)
	if err != nil {
		_ = listener.Close()
		_ = removeOwnedSocket(config.Path, config.UID, created)
		return fail(err)
	}
	server := &Server{
		listener: listener, path: config.Path, uid: config.UID, identity: identity, lock: lock,
		config: config, completed: make(chan Completion, 1), done: make(chan error, 1), statusGate: make(chan struct{}, 1),
	}
	server.wg.Add(1)
	go server.serve()
	return server, nil
}

func validateSocketParent(path string, uid int) error {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path || filepath.Base(path) == "." || filepath.Base(path) == string(os.PathSeparator) {
		return ErrUnsafePath
	}
	dir := filepath.Dir(path)
	if err := rejectSymlinkPath(dir); err != nil {
		return ErrUnsafePath
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return ErrUnsafePath
	}
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 || !ownedBy(info, uid) {
		return ErrUnsafePath
	}
	return nil
}

func rejectSymlinkPath(path string) error {
	for current := filepath.Clean(path); current != string(os.PathSeparator); current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err == nil && info.Mode()&os.ModeSymlink != 0 {
			return ErrUnsafePath
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func acquireSocketLock(path string, uid int) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, ErrUnsafePath
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 || !ownedBy(info, uid) || !singleLink(info) {
		_ = file.Close()
		return nil, ErrUnsafePath
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		return nil, ErrInUse
	}
	return file, nil
}

func unlockAndClose(file *os.File) error {
	if file == nil {
		return nil
	}
	unlockErr := syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	closeErr := file.Close()
	if unlockErr != nil || closeErr != nil {
		return ErrUnavailable
	}
	return nil
}

func inspectSocket(path string, uid int) (socketIdentity, error) {
	return inspectOwnedSocket(path, uid, true)
}

func inspectOwnedSocket(path string, uid int, requirePrivateMode bool) (socketIdentity, error) {
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSocket == 0 || requirePrivateMode && info.Mode().Perm() != 0o600 ||
		!ownedBy(info, uid) || !singleLink(info) {
		return socketIdentity{}, ErrUnsafePath
	}
	device, inode, ok := fileIdentity(info)
	if !ok {
		return socketIdentity{}, ErrUnsafePath
	}
	return socketIdentity{device: device, inode: inode}, nil
}

func removeOwnedSocket(path string, uid int, identity socketIdentity) error {
	current, err := inspectOwnedSocket(path, uid, false)
	if err != nil || current != identity {
		return ErrUnsafePath
	}
	if err := os.Remove(path); err != nil {
		return ErrUnavailable
	}
	return nil
}

func clearStaleSocket(path string, uid int) error {
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return ErrUnsafePath
	}
	identity, err := inspectSocket(path, uid)
	if err != nil {
		return err
	}
	conn, dialErr := net.DialTimeout("unix", path, 50*time.Millisecond)
	if dialErr == nil {
		_ = conn.Close()
		return ErrInUse
	}
	if !errors.Is(dialErr, syscall.ECONNREFUSED) && !errors.Is(dialErr, os.ErrNotExist) {
		return ErrInUse
	}
	current, err := inspectSocket(path, uid)
	if err != nil || current != identity {
		return ErrUnsafePath
	}
	if err := os.Remove(path); err != nil {
		return ErrUnsafePath
	}
	return nil
}

func (s *Server) Completed() <-chan Completion { return s.completed }

// Done yields a non-nil error only if the listener stops unexpectedly.
func (s *Server) Done() <-chan error { return s.done }

func (s *Server) serve() {
	defer s.wg.Done()
	for {
		connection, err := s.listener.AcceptUnix()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				s.done <- nil
			} else {
				s.done <- ErrUnavailable
			}
			close(s.done)
			return
		}
		s.handle(connection)
	}
}

func (s *Server) handle(connection *net.UnixConn) {
	defer connection.Close()
	uid, err := s.config.PeerUID(connection)
	if err != nil || uid != s.uid {
		return
	}
	reader := bufio.NewReaderSize(connection, MaxFrameBytes+1)
	helloData, err := readFrame(connection, reader, s.config.IOTimeout)
	if err != nil || reader.Buffered() != 0 {
		return
	}
	hello, err := parseRequest(helloData, "hello")
	if err != nil {
		return
	}
	status, err := s.snapshot()
	if err != nil || !status.valid() {
		return
	}
	if err := writeFrame(connection, responseFrame{Version: 1, Type: "pong", Nonce: hello.Nonce, Status: status}, s.config.IOTimeout); err != nil {
		return
	}
	closeData, err := readFrame(connection, reader, s.config.IOTimeout)
	if err != nil || reader.Buffered() != 0 {
		return
	}
	closeFrame, err := parseRequest(closeData, "close")
	if err != nil || closeFrame.Nonce != hello.Nonce {
		return
	}
	if err := writeFrame(connection, requestFrame{Version: 1, Type: "closed", Nonce: hello.Nonce}, s.config.IOTimeout); err != nil {
		return
	}
	s.notifyCompletion(status)
}

func recoveryEligible(status Status) bool {
	return status.Runner.State == "exited" && status.Runner.ExitCode != nil && *status.Runner.ExitCode == 75 &&
		!status.Runner.Terminal && !status.Runner.Degraded &&
		(status.Pump.State == "idle" || status.Pump.State == "fresh") &&
		status.Backlog.State != "unknown" && status.Egress.State == "clear" && status.Conductor.State != "unknown"
}

func (s *Server) notifyCompletion(status Status) {
	completion := Completion{RunnerState: status.Runner.State, Recoverable: recoveryEligible(status)}
	if completion.Recoverable {
		select {
		case <-s.completed:
		default:
		}
		s.completed <- completion
		return
	}
	select {
	case s.completed <- completion:
	default:
	}
}

func (s *Server) snapshot() (Status, error) {
	select {
	case s.statusGate <- struct{}{}:
	default:
		return Status{}, ErrUnavailable
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.config.StatusTimeout)
	defer cancel()
	type result struct {
		status Status
		err    error
	}
	resultCh := make(chan result, 1)
	go func() {
		defer func() { <-s.statusGate }()
		status, err := s.config.Snapshot(ctx)
		resultCh <- result{status: status, err: err}
	}()
	select {
	case got := <-resultCh:
		return got.status, got.err
	case <-ctx.Done():
		return Status{}, ctx.Err()
	}
}

func readFrame(connection *net.UnixConn, reader *bufio.Reader, timeout time.Duration) ([]byte, error) {
	if err := connection.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}
	line, err := reader.ReadBytes('\n')
	if err != nil || len(line) == 0 || len(line) > MaxFrameBytes || line[len(line)-1] != '\n' {
		return nil, errors.New("invalid frame")
	}
	line = line[:len(line)-1]
	if !utf8.Valid(line) {
		return nil, errors.New("invalid frame")
	}
	return line, nil
}

func writeFrame(connection *net.UnixConn, value any, timeout time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil || len(data)+1 > MaxFrameBytes {
		return errors.New("invalid frame")
	}
	data = append(data, '\n')
	if err := connection.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	_, err = connection.Write(data)
	return err
}

func (s *Server) Close() error {
	var result error
	s.closeOnce.Do(func() {
		if err := s.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			result = ErrUnavailable
		}
		s.wg.Wait()
		if _, err := os.Lstat(s.path); errors.Is(err, os.ErrNotExist) {
			// Nothing at the owned name remains to remove.
		} else if err != nil {
			result = ErrUnsafePath
		} else if current, inspectErr := inspectSocket(s.path, s.uid); inspectErr != nil || current != s.identity {
			result = ErrUnsafePath
		} else if err := removeOwnedSocket(s.path, s.uid, s.identity); err != nil {
			result = err
		}
		if err := unlockAndClose(s.lock); err != nil {
			result = err
		}
	})
	return result
}

var _ io.Closer = (*Server)(nil)
