//go:build linux

package runner

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	"hop/internal/types"
)

const cgroupBase = "/sys/fs/cgroup/hop"

// wrapCommand on Linux returns the command as-is
// Memory limiting is done via cgroups after process start
func (r *ExecRunner) wrapCommand(command string, memoryLimit uint64) string {
	return command
}

// taskCgroupPath returns the cgroup v2 directory for a task. Keyed by taskID
// (not PID) so we can create and configure it before the process is born.
func taskCgroupPath(taskID string) string {
	return filepath.Join(cgroupBase, taskID)
}

// prepareCgroup creates the per-task cgroup, sets memory.max, and returns
// a directory FD suitable for SysProcAttr.CgroupFD. The kernel then puts
// the child into this cgroup via clone3(CLONE_INTO_CGROUP) at fork time,
// so /proc/self/cgroup is correct from the very first read.
//
// memory.max provides the hard OOM cap. fakeMeminfo (separately) overrides
// /proc/meminfo so apps that don't grok cgroups still see the right
// total — both fire together: cgroup enforces, /proc/meminfo advertises.
//
// Returns -1, nil when memoryLimit == 0 (no cgroup needed).
// Requires Linux ≥ 5.7 and cgroup v2 with the memory controller delegated to cgroupBase.
func (r *ExecRunner) prepareCgroup(taskID string, memoryLimit uint64) (int, error) {
	if memoryLimit == 0 {
		return -1, nil
	}
	path := taskCgroupPath(taskID)
	if err := os.MkdirAll(path, 0755); err != nil {
		return -1, fmt.Errorf("create cgroup: %w", err)
	}
	memMax := filepath.Join(path, "memory.max")
	if err := os.WriteFile(memMax, []byte(fmt.Sprintf("%d", memoryLimit)), 0644); err != nil {
		log.Printf("Warning: failed to set memory.max on %s: %v", path, err)
	}
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC, 0)
	if err != nil {
		return -1, fmt.Errorf("open cgroup dir: %w", err)
	}
	return fd, nil
}

// removeCgroup unlinks the per-task cgroup dir. Safe to call when the task
// never had one (no-op on ENOENT).
func (r *ExecRunner) removeCgroup(taskID string) {
	_ = os.Remove(taskCgroupPath(taskID))
}

// attachCgroup wires a pre-opened cgroup directory FD into the command's
// SysProcAttr so cmd.Start uses clone3(CLONE_INTO_CGROUP).
func (r *ExecRunner) attachCgroup(cmd *exec.Cmd, fd int) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.UseCgroupFD = true
	cmd.SysProcAttr.CgroupFD = fd
}

// ensureCgroupControllers enables +memory in the subtree_control chain so
// per-task cgroups created later actually expose memory.max / memory.current.
// Without this, our writes to memory.max silently no-op and Sparrow-based
// runtimes (RavenDB) see "no cgroup limit" — which is fine for functionality
// (they fall back to /proc/meminfo which fakeMeminfo overrides) but disables
// the kernel-level OOM hard cap and leaves the cgroup-limit widget blank.
//
// Best-effort: if the root cgroup is owned by systemd we may get EACCES, in
// which case the operator must enable +memory themselves (or run hop under a
// delegated slice). We log and continue.
func ensureCgroupControllers() {
	if err := os.WriteFile("/sys/fs/cgroup/cgroup.subtree_control", []byte("+memory"), 0644); err != nil {
		log.Printf("Warning: could not enable +memory on root cgroup: %v (continuing — set it manually or via systemd delegation)", err)
	}
	if err := os.MkdirAll(cgroupBase, 0755); err != nil {
		log.Printf("Warning: could not create %s: %v", cgroupBase, err)
		return
	}
	if err := os.WriteFile(cgroupBase+"/cgroup.subtree_control", []byte("+memory"), 0644); err != nil {
		log.Printf("Warning: could not enable +memory on %s: %v", cgroupBase, err)
	}
}

// mountVolume bind-mounts a host path into the task directory.
// The target must exist before mount(2) — directory for dir sources, file for file sources.
func (r *ExecRunner) mountVolume(hostPath, targetPath string) error {
	info, err := os.Stat(hostPath)
	if err != nil {
		return err
	}
	if info.IsDir() {
		if err := os.MkdirAll(targetPath, 0755); err != nil {
			return err
		}
	} else {
		if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
			return err
		}
		f, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		_ = f.Close()
	}
	return syscall.Mount(hostPath, targetPath, "", syscall.MS_BIND, "")
}

// unmountVolume cleans up a mounted volume. MNT_DETACH (lazy unmount) keeps
// cleanup safe even when something inside still holds a reference — without it,
// EBUSY would leave the bind mount in place and a subsequent RemoveAll on
// taskDir would walk into the bound source and delete files from the host.
func (r *ExecRunner) unmountVolume(targetPath string) error {
	return syscall.Unmount(targetPath, syscall.MNT_DETACH)
}

// setupCommand configures the command with optional namespace isolation
func (r *ExecRunner) setupCommand(job *types.Job, taskDir string, portEnvVars []string) *exec.Cmd {
	command := r.wrapCommand(job.Command, job.MemoryLimit)
	cmd := exec.Command("/bin/sh", "-c", command)

	if r.config.Isolate {
		// Full isolation: chroot + namespaces (container-like)
		cmd.Dir = "/"
		cmd.Env = []string{
			"HOME=/",
			"TMPDIR=/tmp",
			"PATH=/bin:/usr/bin",
		}
		cmd.SysProcAttr = &syscall.SysProcAttr{
			Chroot:  taskDir,
			Setpgid: true,
			Cloneflags: syscall.CLONE_NEWPID | // Own PID namespace (PID 1 inside)
				syscall.CLONE_NEWNS | // Own mount namespace
				syscall.CLONE_NEWUTS | // Own hostname
				syscall.CLONE_NEWIPC, // Own IPC namespace
			// Note: CLONE_NEWNET omitted - requires veth setup
		}
	} else {
		// Non-isolated mode
		sysProcAttr := &syscall.SysProcAttr{
			Setpgid: true,
		}
		if job.User != "" {
			cred, _, err := lookupCredential(job.User)
			if err != nil {
				log.Printf("Warning: %v, running as current user", err)
			} else {
				sysProcAttr.Credential = cred
			}
		}
		cmd.Dir = taskDir
		cmd.Env = []string{
			fmt.Sprintf("HOME=%s", taskDir),
			fmt.Sprintf("TMPDIR=%s/tmp", taskDir),
			"PATH=/usr/local/bin:/usr/bin:/bin",
		}
		cmd.SysProcAttr = sysProcAttr
	}

	cmd.Env = append(cmd.Env, portEnvVars...)
	for k, v := range job.Env {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}
	cmd.Env = append(cmd.Env, AttrEnvVars(r.config.NodeAttrs)...)

	return cmd
}

// setupIsolationEnv prepares the chroot with bind mounts of system paths so
// /bin/sh, shared libraries, /dev/null, /dev/urandom and /proc all resolve
// correctly inside the jail. Absolute symlinks would break here: once chroot
// is applied, a target like "/bin/sh" resolves back to taskDir/bin/sh — the
// link itself — and the kernel returns ELOOP. Bind mounts work because they
// propagate into the child's mount namespace via CLONE_NEWNS.
//
// /dev must stay writable so writes to char devices (/dev/null, /dev/random,
// /dev/shm) keep working — runtimes like CoreCLR fail without that.
// /proc is bind-mounted from the host: the new PID namespace would ideally
// get a fresh procfs after unshare, but mounting in the parent is enough for
// /proc/self and most introspection .NET / glibc rely on.
func (r *ExecRunner) setupIsolationEnv(taskDir string) []string {
	binds := []struct {
		src      string
		readonly bool
	}{
		{"/bin", true},
		{"/usr", true},
		{"/lib", true},
		{"/lib64", true},
		{"/dev", false},
		{"/proc", true},
		{"/sys", true},
	}
	var mounted []string
	for _, b := range binds {
		if _, err := os.Stat(b.src); err != nil {
			continue
		}
		target := filepath.Join(taskDir, b.src)
		if err := os.MkdirAll(target, 0755); err != nil {
			log.Printf("Warning: failed to create bind target %s: %v", target, err)
			continue
		}
		if err := syscall.Mount(b.src, target, "", syscall.MS_BIND|syscall.MS_REC, ""); err != nil {
			log.Printf("Warning: failed to bind-mount %s into chroot: %v", b.src, err)
			continue
		}
		mounted = append(mounted, target)
		if b.readonly {
			if err := syscall.Mount("", target, "", syscall.MS_REMOUNT|syscall.MS_BIND|syscall.MS_RDONLY, ""); err != nil {
				log.Printf("Warning: failed to remount %s read-only: %v", target, err)
			}
		}
	}

	// Single-file bind for resolv.conf so the rest of /etc (shadow, passwd, …)
	// stays hidden from the task.
	if _, err := os.Stat("/etc/resolv.conf"); err == nil {
		etcDir := filepath.Join(taskDir, "etc")
		if err := os.MkdirAll(etcDir, 0755); err == nil {
			target := filepath.Join(etcDir, "resolv.conf")
			if f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY, 0644); err == nil {
				_ = f.Close()
				if err := syscall.Mount("/etc/resolv.conf", target, "", syscall.MS_BIND, ""); err == nil {
					mounted = append(mounted, target)
				}
			}
		}
	}

	return mounted
}
