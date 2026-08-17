//go:build linux

package ampacp

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// The native ownership handoff re-reads every descriptor it is about to trust,
// so each syscall it depends on is reached through a seam. Faulting a seam is
// the only way to prove the handoff fails closed when the kernel stops
// answering for a descriptor it already accepted.
var (
	nativeOwnershipOpenFilesystemRoot = func() (int, error) {
		return unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	}
	nativeOwnershipFstat   = unix.Fstat
	nativeOwnershipClose   = unix.Close
	nativeOwnershipReadDir = func(directory *os.File) ([]os.DirEntry, error) {
		return directory.ReadDir(-1)
	}
	nativeOwnershipFtruncate = unix.Ftruncate
	nativeOwnershipFchown    = unix.Fchown
	nativeOwnershipWrite     = func(file *os.File, contents []byte) (int, error) { return file.Write(contents) }
	nativeOwnershipSync      = func(file *os.File) error { return file.Sync() }
)

func handoffGeneratedNativeTreePlatform(root string, uid uint32, gid uint32) error {
	trustedUID := effectiveUID()
	trustedGID := effectiveGID()

	directory, err := openNativeOwnershipDirectory(root, func(stat unix.Stat_t, final bool) error {
		return validateGeneratedNativeAncestor(stat, final, trustedUID, trustedGID, uid, gid)
	})
	if err != nil {
		return fmt.Errorf("open generated native tree: %w", err)
	}
	defer directory.Close()

	if err := handoffNativeOwnershipDirectory(directory, trustedUID, trustedGID, uid, gid); err != nil {
		return fmt.Errorf("handoff generated native tree: %w", err)
	}

	return nil
}

func openNativeOwnershipDirectory(name string, validate func(unix.Stat_t, bool) error) (*os.File, error) {
	if !filepath.IsAbs(name) {
		return nil, errors.New("native path must be absolute")
	}

	clean := filepath.Clean(name)

	fd, err := nativeOwnershipOpenFilesystemRoot()
	if err != nil {
		return nil, err
	}

	components := strings.Split(strings.TrimPrefix(clean, "/"), "/")

	var rootStat unix.Stat_t
	if statErr := nativeOwnershipFstat(fd, &rootStat); statErr != nil {
		_ = unix.Close(fd)

		return nil, statErr
	}

	if validateErr := validate(rootStat, len(components) == 1 && components[0] == ""); validateErr != nil {
		_ = unix.Close(fd)

		return nil, validateErr
	}

	for index, component := range components {
		if component == "" {
			continue
		}

		next, openErr := unix.Openat(fd, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if openErr != nil {
			_ = unix.Close(fd)

			return nil, openErr
		}

		var stat unix.Stat_t
		if statErr := nativeOwnershipFstat(next, &stat); statErr != nil {
			_ = unix.Close(next)
			_ = unix.Close(fd)

			return nil, statErr
		}

		if validateErr := validate(stat, index == len(components)-1); validateErr != nil {
			_ = unix.Close(next)
			_ = unix.Close(fd)

			return nil, validateErr
		}

		closeErr := nativeOwnershipClose(fd)
		if closeErr != nil {
			_ = unix.Close(next)

			return nil, closeErr
		}

		fd = next
	}

	return os.NewFile(uintptr(fd), clean), nil
}

// nativeSharedIdentityAncestryRemedy states what an operator can change when
// the trusted identity is also the target identity and an ancestor still fails
// the walk. There is no privilege boundary left to lean on in that shape, so the
// remaining answers are to give the supervisor one, or to anchor the tree on a
// path the identity already owns.
const nativeSharedIdentityAncestryRemedy = "run the supervisor as root to isolate the agent identity, " +
	"or place the native directory under a path the agent identity owns"

// nativeAncestorIsSharedIdentityRoot reports whether a non-final ancestor is
// acceptable purely because root owns it. It is, only when the trusted identity
// and the target identity are the same one: owning both ends of the handoff
// leaves no privilege boundary for the ancestry rule to defend, while every path
// to a directory that identity owns still crosses root-owned components such as
// "/" and "/home". An ancestor owned by any other identity stays refused, so a
// second local identity still cannot interpose one.
func nativeAncestorIsSharedIdentityRoot(stat unix.Stat_t, final bool, trustedUID uint32, targetUID uint32) bool {
	return !final && trustedUID == targetUID && stat.Uid == 0 && stat.Gid == 0
}

func validateGeneratedNativeAncestor(
	stat unix.Stat_t,
	final bool,
	trustedUID uint32,
	trustedGID uint32,
	targetUID uint32,
	targetGID uint32,
) error {
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR {
		return errors.New("generated native path ancestry is not a trusted directory")
	}

	trusted := stat.Uid == trustedUID && stat.Gid == trustedGID

	rootOwned := nativeAncestorIsSharedIdentityRoot(stat, final, trustedUID, targetUID)
	if !trusted && !rootOwned {
		if !final && trustedUID == targetUID {
			return fmt.Errorf(
				"generated native path ancestor is uid=%d gid=%d; %s",
				stat.Uid, stat.Gid, nativeSharedIdentityAncestryRemedy,
			)
		}

		return errors.New("generated native path ancestry is not a trusted directory")
	}

	mode := stat.Mode & 0o7777
	if final && mode != 0o700 {
		return fmt.Errorf("generated native root mode %#o is unsafe", mode)
	}

	if !final && mode&0o022 != 0 && mode&unix.S_ISVTX == 0 {
		return fmt.Errorf("generated native ancestor mode %#o is writable without sticky protection", mode)
	}

	if !final && !nativeIdentityCanTraverse(stat, targetUID, targetGID) {
		return errors.New("generated native path ancestry is not traversable by the target identity")
	}

	return nil
}

func validateDurableNativeAncestor(
	stat unix.Stat_t,
	final bool,
	trustedUID uint32,
	trustedGID uint32,
	targetUID uint32,
	targetGID uint32,
) error {
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR {
		return errors.New("native-owned path ancestry is not a directory")
	}

	trusted := stat.Uid == trustedUID && stat.Gid == trustedGID

	rootOwned := nativeAncestorIsSharedIdentityRoot(stat, final, trustedUID, targetUID)

	target := stat.Uid == targetUID && stat.Gid == targetGID
	if !trusted && !target && !rootOwned {
		if !final && trustedUID == targetUID {
			return fmt.Errorf(
				"native-owned path ancestor is uid=%d gid=%d; %s",
				stat.Uid, stat.Gid, nativeSharedIdentityAncestryRemedy,
			)
		}

		return fmt.Errorf("native-owned path ancestor is uid=%d gid=%d", stat.Uid, stat.Gid)
	}

	mode := stat.Mode & 0o7777
	if mode&0o022 != 0 && ((!trusted && !rootOwned) || mode&unix.S_ISVTX == 0) {
		return fmt.Errorf("native-owned path ancestor mode %#o is writable", mode)
	}

	if final && (!target || mode&0o700 != 0o700) {
		return errors.New("native-owned directory is not safely owned by the target identity")
	}

	if !nativeIdentityCanTraverse(stat, targetUID, targetGID) {
		return errors.New("native-owned path ancestry is not traversable by the target identity")
	}

	return nil
}

func nativeIdentityCanTraverse(stat unix.Stat_t, uid uint32, gid uint32) bool {
	switch {
	case stat.Uid == uid:
		return stat.Mode&0o100 != 0
	case stat.Gid == gid:
		return stat.Mode&0o010 != 0
	default:
		return stat.Mode&0o001 != 0
	}
}

func writeNativeOwnedFilePlatform(path string, contents []byte, uid uint32, gid uint32) error {
	parent := filepath.Dir(path)
	trustedUID := effectiveUID()
	trustedGID := effectiveGID()

	directory, err := openNativeOwnershipDirectory(parent, func(stat unix.Stat_t, final bool) error {
		return validateDurableNativeAncestor(stat, final, trustedUID, trustedGID, uid, gid)
	})
	if err != nil {
		return err
	}
	defer directory.Close()

	name := filepath.Base(path)
	fd, err := unix.Openat(int(directory.Fd()), name, unix.O_WRONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	created := false

	if errors.Is(err, unix.ENOENT) {
		fd, err = unix.Openat(
			int(directory.Fd()),
			name,
			unix.O_WRONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_CREAT|unix.O_EXCL,
			0o600,
		)
		created = err == nil
	}

	if err != nil {
		return err
	}

	file := os.NewFile(uintptr(fd), path)
	defer file.Close()

	var stat unix.Stat_t
	if err := nativeOwnershipFstat(fd, &stat); err != nil {
		return err
	}

	wantUID, wantGID := uid, gid
	if created {
		wantUID, wantGID = trustedUID, trustedGID
	}

	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Mode&0o7777 != 0o600 || stat.Nlink != 1 || stat.Uid != wantUID || stat.Gid != wantGID {
		return errors.New("native-owned file is unsafe")
	}

	if err := nativeOwnershipFtruncate(fd, 0); err != nil {
		return err
	}

	if _, err := nativeOwnershipWrite(file, contents); err != nil {
		return err
	}

	if created {
		if err := nativeOwnershipFchown(fd, int(uid), int(gid)); err != nil {
			return err
		}
	}

	if err := nativeOwnershipSync(file); err != nil {
		return err
	}

	return chownAndVerifyNativeInode(fd, unix.S_IFREG, uid, gid, true)
}

func handoffNativeOwnershipDirectory(
	directory *os.File,
	trustedUID uint32,
	trustedGID uint32,
	targetUID uint32,
	targetGID uint32,
) error {
	if err := validateHandoffNativeInode(int(directory.Fd()), unix.S_IFDIR, trustedUID, trustedGID, targetUID, targetGID, false); err != nil {
		return err
	}

	entries, err := nativeOwnershipReadDir(directory)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}

	for _, entry := range entries {
		name := entry.Name()
		if name == "." || name == handoffParentDir || strings.ContainsRune(name, '/') {
			return fmt.Errorf("invalid generated native entry %q", name)
		}

		fd, openErr := unix.Openat(
			int(directory.Fd()),
			name,
			unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK,
			0,
		)
		if openErr != nil {
			return fmt.Errorf("open generated native entry %q: %w", name, openErr)
		}

		entryFile := os.NewFile(uintptr(fd), name)
		entryErr := handoffNativeOwnershipEntry(entryFile, trustedUID, trustedGID, targetUID, targetGID)

		closeErr := entryFile.Close()
		if entryErr != nil || closeErr != nil {
			return errors.Join(fmt.Errorf("handoff generated native entry %q: %w", name, entryErr), closeErr)
		}
	}

	return chownAndVerifyNativeInode(int(directory.Fd()), unix.S_IFDIR, targetUID, targetGID, false)
}

func handoffNativeOwnershipEntry(
	entry *os.File,
	trustedUID uint32,
	trustedGID uint32,
	targetUID uint32,
	targetGID uint32,
) error {
	var stat unix.Stat_t
	if err := nativeOwnershipFstat(int(entry.Fd()), &stat); err != nil {
		return err
	}

	switch stat.Mode & unix.S_IFMT {
	case unix.S_IFDIR:
		return handoffNativeOwnershipDirectory(entry, trustedUID, trustedGID, targetUID, targetGID)
	case unix.S_IFREG:
		if err := validateHandoffNativeInode(int(entry.Fd()), unix.S_IFREG, trustedUID, trustedGID, targetUID, targetGID, true); err != nil {
			return err
		}

		return chownAndVerifyNativeInode(int(entry.Fd()), unix.S_IFREG, targetUID, targetGID, true)
	default:
		return fmt.Errorf("generated native inode has unsupported type %#o", stat.Mode&unix.S_IFMT)
	}
}

func validateHandoffNativeInode(
	fd int,
	kind uint32,
	trustedUID uint32,
	trustedGID uint32,
	targetUID uint32,
	targetGID uint32,
	singleLink bool,
) error {
	var stat unix.Stat_t
	if err := nativeOwnershipFstat(fd, &stat); err != nil {
		return err
	}

	if stat.Mode&unix.S_IFMT != kind {
		return fmt.Errorf("generated native inode type %#o changed", stat.Mode&unix.S_IFMT)
	}

	if stat.Uid != trustedUID || stat.Gid != trustedGID {
		return fmt.Errorf("generated native inode owner changed to uid=%d gid=%d", stat.Uid, stat.Gid)
	}

	if singleLink && stat.Nlink != 1 {
		return fmt.Errorf("generated native file has %d links", stat.Nlink)
	}

	mode := stat.Mode & 0o7777
	if kind == unix.S_IFDIR && mode != 0o700 {
		return fmt.Errorf("generated native directory mode %#o is unsafe", mode)
	}

	if kind == unix.S_IFREG && mode != 0o600 && mode != 0o700 {
		return fmt.Errorf("generated native inode mode %#o is unsafe", stat.Mode&0o7777)
	}

	return nil
}

func chownAndVerifyNativeInode(fd int, kind uint32, uid uint32, gid uint32, singleLink bool) error {
	if err := unix.Fchown(fd, int(uid), int(gid)); err != nil {
		return err
	}

	var stat unix.Stat_t
	if err := nativeOwnershipFstat(fd, &stat); err != nil {
		return err
	}

	if stat.Mode&unix.S_IFMT != kind || stat.Uid != uid || stat.Gid != gid {
		return errors.New("generated native inode ownership handoff could not be proven")
	}

	if singleLink && stat.Nlink != 1 {
		return fmt.Errorf("generated native file has %d links after handoff", stat.Nlink)
	}

	return nil
}

// Seams for the fail-closed guards below. Linux cannot produce a uid or gid
// outside the 32 bits it stores them in, so the guards are unreachable through
// the real syscalls; tests swap these to reach them.
var (
	effectiveUIDSource = os.Geteuid
	effectiveGIDSource = os.Getegid
)

// effectiveUID reports the caller's effective UID. Linux stores UIDs in 32
// bits, so the int os.Geteuid returns always fits and the guard never fires; it
// is here because every caller compares this value against an inode owner,
// where a silently truncated match would grant trust instead of withholding it.
// The unrepresentable case therefore fails closed on an ID no inode can carry.
func effectiveUID() uint32 {
	uid := effectiveUIDSource()
	if uid < 0 || uid > math.MaxUint32 {
		return math.MaxUint32
	}

	return uint32(uid)
}

// effectiveGID reports the caller's effective GID under the same contract as
// effectiveUID.
func effectiveGID() uint32 {
	gid := effectiveGIDSource()
	if gid < 0 || gid > math.MaxUint32 {
		return math.MaxUint32
	}

	return uint32(gid)
}
