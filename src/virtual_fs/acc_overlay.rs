// IIASA ACCELERATOR PLATFORM USECASE: overlay features for the wkube job agent.
//
// Kept in this file so the fork stays a few call sites away from upstream. Everything
// here is active only in overlay mode with ACCELERATOR_MOUNT set.
//
// 1. `.hf_overlay_writes`: a read-only virtual file at the mount root listing every
//    overlay file as `<relative path>:<gid>:<mtime>`, with the gid and mtime (Unix
//    seconds) read from disk. The agent uses it to find the files its job wrote
//    (gid == JOB_GID). The listing is taken once per open and every read of that
//    handle is served from the same snapshot: readers pull it in several chunks, and
//    rebuilding it per read would shift offsets whenever a parallel job adds or
//    removes a file, cutting, duplicating or dropping lines.
//
// 2. Ownership follows the writer, on disk only: the mount pod runs hf-mount as root,
//    so overlay files start as 0:0 on disk. A new file goes through FUSE `create`, but
//    rewriting an existing file goes through `open` (or `setattr` for truncate) and
//    would keep its previous owner, so a retry or a second job writing the same path
//    would never claim it. Every create, write-open and truncate re-stamps the file on
//    disk with the caller's uid/gid: the last writer owns the file. This on-disk owner
//    is what `.hf_overlay_writes` reports. What hf-mount reports to the kernel is left
//    exactly as upstream (the creator for new files, `--uid`/`--gid` otherwise).
//
// 3. No idmapped mounts: the claim in (2) needs the real uid/gid of whoever opens or
//    truncates a file, which newer kernels stop sending once idmapped mounts are
//    allowed. See `acc_allow_idmap` for the trade-off.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::SystemTime;

use super::inode::{InodeKind, ROOT_INODE};
use super::{VirtualFs, VirtualFsAttr};
use crate::overlay::OverlayBacking;

pub(crate) const OVERLAY_WRITES_NAME: &str = ".hf_overlay_writes";
pub(crate) const OVERLAY_WRITES_INO: u64 = u64::MAX - 1;

// The listing is generated on open, so the real size is unknown at lookup time.
// Report a size large enough for any listing; the file is opened with DIRECT_IO and
// reads past the end return no data.
const OVERLAY_WRITES_SIZE: u64 = 1024 * 1024 * 1024;

// File handles for open index snapshots. Real handles come from alloc_file_handle,
// which counts up from 1 and never reaches this range.
const OVERLAY_WRITES_FH_BASE: u64 = 1 << 62;
static OVERLAY_WRITES_NEXT_FH: AtomicU64 = AtomicU64::new(OVERLAY_WRITES_FH_BASE);
static OVERLAY_WRITES_SNAPSHOTS: LazyLock<Mutex<HashMap<u64, Arc<Vec<u8>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn is_overlay_writes_fh(fh: u64) -> bool {
    fh >= OVERLAY_WRITES_FH_BASE
}

#[cfg(test)]
thread_local! {
    static TEST_ACCELERATOR_MOUNT: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
}

fn accelerator_mount() -> bool {
    #[cfg(test)]
    if let Some(enabled) = TEST_ACCELERATOR_MOUNT.with(|c| c.get()) {
        return enabled;
    }
    std::env::var("ACCELERATOR_MOUNT").is_ok()
}

/// Whether hf-mount may ask the kernel for FUSE_ALLOW_IDMAP (idmapped mounts) at mount
/// time. Upstream always asks; on the accelerator platform (ACCELERATOR_MOUNT set) we
/// do not.
///
/// What the flag is for: a pod with `hostUsers: false` runs in its own user namespace,
/// so uid 65534 inside the pod is some other, large uid on the host. The kubelet then
/// mounts the volume into the pod as an "idmapped mount" that translates ids in both
/// directions, and the kernel only allows that for a FUSE filesystem that negotiated
/// FUSE_ALLOW_IDMAP. `hostUsers` itself only has an effect on Kubernetes versions where
/// the UserNamespacesSupport feature is enabled; elsewhere it is ignored and every pod
/// shares the host's ids (`hostUsers: true` behaviour).
///
/// The cost of negotiating it: from Linux 6.12 on, once the flag is negotiated, the
/// kernel sends the caller's real uid/gid only for requests that create something
/// (create, mkdir, symlink, mknod). Every other request (open, setattr, rename, ...)
/// arrives with uid/gid set to -1 (4294967295). `claim_overlay_owner` would then have
/// no way to know which job rewrote or truncated a file, and stamping that -1 into the
/// inode made the kernel refuse later writes (EACCES) and deletes (EOVERFLOW) of that
/// file. Older kernels do not know the flag, so for them nothing changes either way.
///
/// The accelerator platform does not run job pods in user namespaces: jobs get their
/// identity from the pod securityContext (runAsUser 65534, runAsGroup = per-job gid in
/// 100000-999999, fsGroup 65534), and the job pod's /proc/self/uid_map is the identity
/// map `0 0 4294967295`. A user namespace could not even represent those large gids,
/// since a pod gets only 65536 ids.
///
/// Advantages of not asking for it:
/// - every request carries the caller's real uid/gid on every kernel version, so the
///   ownership claim on rewrite and truncate works the same everywhere;
/// - no -1 owners, so writes, renames and deletes behave normally.
///
/// Disadvantages:
/// - pods that really run in a user namespace (`hostUsers: false` on a cluster with
///   UserNamespacesSupport enabled) cannot mount this volume while ACCELERATOR_MOUNT is
///   set; if jobs are ever moved to user namespaces, this has to be revisited;
/// - one more behaviour difference from upstream hf-mount.
///
/// Without ACCELERATOR_MOUNT hf-mount asks for the flag exactly as upstream does.
#[cfg_attr(not(feature = "fuse"), allow(dead_code))]
pub(crate) fn acc_allow_idmap() -> bool {
    !accelerator_mount()
}

impl VirtualFs {
    fn acc_overlay_enabled(&self) -> bool {
        self.overlay_backing.is_some() && accelerator_mount()
    }

    fn overlay_writes_attr(&self) -> VirtualFsAttr {
        // Always the current time, so readers never treat the listing as unchanged.
        let now = SystemTime::now();
        VirtualFsAttr {
            ino: OVERLAY_WRITES_INO,
            size: OVERLAY_WRITES_SIZE,
            blocks: OVERLAY_WRITES_SIZE.div_ceil(512),
            atime: now,
            mtime: now,
            ctime: now,
            kind: InodeKind::File,
            perm: 0o444,
            nlink: 1,
            uid: self.uid,
            gid: self.gid,
        }
    }

    /// `lookup` hook: the virtual file at the mount root.
    pub(crate) fn acc_overlay_writes_lookup(&self, parent: u64, name: &str) -> Option<VirtualFsAttr> {
        (parent == ROOT_INODE && name == OVERLAY_WRITES_NAME && self.acc_overlay_enabled())
            .then(|| self.overlay_writes_attr())
    }

    /// `getattr` hook for the virtual file's inode.
    pub(crate) fn acc_overlay_writes_getattr(&self, ino: u64) -> Option<VirtualFsAttr> {
        (ino == OVERLAY_WRITES_INO && self.acc_overlay_enabled()).then(|| self.overlay_writes_attr())
    }

    /// `open` hook: snapshot the listing and return a handle for reading it.
    #[cfg_attr(not(feature = "fuse"), allow(dead_code))]
    pub(crate) fn acc_overlay_writes_open(&self) -> u64 {
        let listing = match self.overlay_backing.as_deref() {
            Some(overlay) => list_overlay_writes(overlay),
            None => String::new(),
        };
        let fh = OVERLAY_WRITES_NEXT_FH.fetch_add(1, Ordering::Relaxed);
        OVERLAY_WRITES_SNAPSHOTS
            .lock()
            .expect("overlay writes snapshots poisoned")
            .insert(fh, Arc::new(listing.into_bytes()));
        fh
    }

    /// `read` hook: the requested slice of the handle's snapshot, or None for any
    /// handle that is not an index handle.
    #[cfg_attr(not(feature = "fuse"), allow(dead_code))]
    pub(crate) fn acc_overlay_writes_read(&self, fh: u64, offset: u64, size: u32) -> Option<Vec<u8>> {
        if !is_overlay_writes_fh(fh) {
            return None;
        }
        let snapshot = OVERLAY_WRITES_SNAPSHOTS
            .lock()
            .expect("overlay writes snapshots poisoned")
            .get(&fh)
            .cloned();
        let Some(bytes) = snapshot else {
            return Some(Vec::new());
        };
        let start = (offset as usize).min(bytes.len());
        let end = start.saturating_add(size as usize).min(bytes.len());
        Some(bytes[start..end].to_vec())
    }

    /// `release` hook: drop the handle's snapshot. Returns false for any handle that
    /// is not an index handle.
    #[cfg_attr(not(feature = "fuse"), allow(dead_code))]
    pub(crate) fn acc_overlay_writes_release(&self, fh: u64) -> bool {
        if !is_overlay_writes_fh(fh) {
            return false;
        }
        OVERLAY_WRITES_SNAPSHOTS
            .lock()
            .expect("overlay writes snapshots poisoned")
            .remove(&fh);
        true
    }

    /// Make `uid:gid` the owner of the overlay file behind `ino` on disk. Called after a
    /// successful create, write-open or truncate; never fails the caller's operation.
    ///
    /// Only the file on disk in the overlay backing directory is changed; the inode table
    /// is left alone. That is enough for everything the job owner is needed for: the
    /// wkube job agent decides which files to upload and which leftovers to clean up from
    /// `.hf_overlay_writes`, and that listing takes the gid from the file on disk (see
    /// `file_gid`), never from the inode table. So a retried or restarted job that
    /// rewrites a file left by an earlier run becomes its owner and syncs it.
    ///
    /// The inode table is not touched because nothing that matters reads the owner from
    /// it, and changing what the kernel is told about an existing file would only risk
    /// breaking permission checks. The kernel keeps seeing what upstream reports: the
    /// creator for files created in this session, the mount's `--uid`/`--gid` otherwise.
    /// Use `.hf_overlay_writes` to see which job owns a file.
    ///
    /// A uid or gid of -1 (4294967295) means the kernel did not say who the caller is
    /// (see `acc_allow_idmap`); the claim is skipped rather than stamping an invalid id.
    pub(crate) fn claim_overlay_owner(&self, ino: u64, uid: u32, gid: u32) {
        if !self.acc_overlay_enabled() || uid == u32::MAX || gid == u32::MAX {
            return;
        }
        let Some(overlay) = self.overlay_backing.as_deref() else {
            return;
        };

        let full_path = {
            let inodes = self.inode_table.read().expect("inodes poisoned");
            let Some(entry) = inodes.get(ino) else {
                return;
            };
            entry.full_path.clone()
        };

        if let Err(e) = set_owner(overlay, &full_path, uid, gid) {
            tracing::warn!("Failed to set overlay owner of {} to {}:{}: {}", full_path, uid, gid, e);
        }
    }
}

/// Every regular file in the overlay as `<relative path>:<gid>:<mtime>`, one per line,
/// with the mtime in seconds since the Unix epoch.
fn list_overlay_writes(overlay: &OverlayBacking) -> String {
    let mut lines = Vec::new();
    let mut dirs = vec![String::new()];
    while let Some(dir) = dirs.pop() {
        let Ok(entries) = overlay.read_dir(&dir) else {
            continue;
        };
        for entry in entries {
            if entry.is_symlink {
                continue;
            }
            let path = if dir.is_empty() {
                entry.name
            } else {
                format!("{}/{}", dir, entry.name)
            };
            if entry.is_dir {
                dirs.push(path);
            } else if let Some(gid) = file_gid(overlay, &path) {
                let mtime = entry
                    .mtime
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map_or(0, |d| d.as_secs());
                lines.push(format!("{}:{}:{}", path, gid, mtime));
            }
        }
    }
    lines.join("\n")
}

#[cfg(unix)]
fn file_gid(overlay: &OverlayBacking, path: &str) -> Option<u32> {
    use std::os::unix::fs::MetadataExt;
    let meta = overlay
        .open_file(path, true, false, false, false)
        .ok()?
        .metadata()
        .ok()?;
    meta.is_file().then(|| meta.gid())
}

#[cfg(not(unix))]
fn file_gid(_overlay: &OverlayBacking, _path: &str) -> Option<u32> {
    Some(0)
}

#[cfg(unix)]
fn set_owner(overlay: &OverlayBacking, full_path: &str, uid: u32, gid: u32) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::MetadataExt;

    // `exists` is false for symlinks, so chown never follows one.
    if !overlay.exists(full_path)? {
        return Ok(());
    }
    let file = overlay.open_file(full_path, true, false, false, false)?;
    let meta = file.metadata()?;
    if !meta.is_file() || (meta.uid() == uid && meta.gid() == gid) {
        return Ok(());
    }
    if unsafe { libc::fchown(file.as_raw_fd(), uid, gid) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(not(unix))]
fn set_owner(_overlay: &OverlayBacking, _full_path: &str, _uid: u32, _gid: u32) -> std::io::Result<()> {
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use std::os::unix::fs::MetadataExt;

    use super::{OVERLAY_WRITES_INO, OVERLAY_WRITES_NAME, TEST_ACCELERATOR_MOUNT};
    use crate::test_mocks::{MockHub, MockXet, make_overlay_test_vfs_with_root};
    use crate::virtual_fs::inode::ROOT_INODE;

    fn overlay_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("hf_acc_overlay_{}_{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn with_accelerator<R>(enabled: bool, f: impl FnOnce() -> R) -> R {
        TEST_ACCELERATOR_MOUNT.with(|c| c.set(Some(enabled)));
        let out = f();
        TEST_ACCELERATOR_MOUNT.with(|c| c.set(None));
        out
    }

    fn running_as_root() -> bool {
        unsafe { libc::geteuid() == 0 }
    }

    fn disk_gid(root: &std::path::Path, name: &str) -> u32 {
        std::fs::metadata(root.join(name)).unwrap().gid()
    }

    fn read_listing(t: &crate::test_mocks::OverlayTestVfs) -> String {
        let fh = t.vfs.acc_overlay_writes_open();
        let data = t.vfs.acc_overlay_writes_read(fh, 0, u32::MAX).unwrap();
        t.vfs.acc_overlay_writes_release(fh);
        String::from_utf8(data).unwrap()
    }

    #[test]
    fn writes_index_lists_files_with_their_gid() {
        let root = overlay_dir("index");
        std::fs::write(root.join("a.txt"), b"hello").unwrap();
        std::fs::create_dir_all(root.join("sub/deeper")).unwrap();
        std::fs::write(root.join("sub/deeper/b.txt"), b"world").unwrap();
        std::os::unix::fs::symlink("a.txt", root.join("link.txt")).unwrap();
        let set_mtime = |name: &str, secs: u64| {
            let file = std::fs::File::options().write(true).open(root.join(name)).unwrap();
            file.set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(secs))
                .unwrap();
        };
        set_mtime("a.txt", 1_761_000_000);
        set_mtime("sub/deeper/b.txt", 1_761_000_123);
        let gid = disk_gid(&root, "a.txt");
        let t = make_overlay_test_vfs_with_root(MockHub::new(), MockXet::new(), root);

        let mut lines: Vec<String> = read_listing(&t).lines().map(str::to_owned).collect();
        lines.sort();
        assert_eq!(
            lines,
            vec![
                format!("a.txt:{gid}:1761000000"),
                format!("sub/deeper/b.txt:{gid}:1761000123"),
            ]
        );
    }

    #[test]
    fn writes_index_serves_slices() {
        let root = overlay_dir("slices");
        std::fs::write(root.join("a.txt"), b"x").unwrap();
        let t = make_overlay_test_vfs_with_root(MockHub::new(), MockXet::new(), root);

        let full = read_listing(&t).into_bytes();
        let fh = t.vfs.acc_overlay_writes_open();
        assert_eq!(t.vfs.acc_overlay_writes_read(fh, 2, 3).unwrap(), full[2..5].to_vec());
        assert!(t.vfs.acc_overlay_writes_read(fh, 10_000, 10).unwrap().is_empty());
        assert!(t.vfs.acc_overlay_writes_release(fh));
        assert!(
            t.vfs.acc_overlay_writes_read(42, 0, 10).is_none(),
            "other handles are not intercepted"
        );
        assert!(
            !t.vfs.acc_overlay_writes_release(42),
            "other handles are not intercepted"
        );
    }

    #[test]
    fn writes_index_snapshot_is_stable_while_files_change() {
        let root = overlay_dir("snapshot");
        for i in 0..50 {
            std::fs::write(root.join(format!("file{i:02}.rds")), b"x").unwrap();
        }
        let t = make_overlay_test_vfs_with_root(MockHub::new(), MockXet::new(), root);

        // Read one handle in small chunks while a parallel job adds and removes files.
        let fh = t.vfs.acc_overlay_writes_open();
        let mut chunked = Vec::new();
        let mut offset = 0u64;
        let mut round = 0;
        loop {
            let chunk = t.vfs.acc_overlay_writes_read(fh, offset, 64).unwrap();
            if chunk.is_empty() {
                break;
            }
            offset += chunk.len() as u64;
            chunked.extend_from_slice(&chunk);
            std::fs::write(t.overlay_root.join(format!("aaa_new{round}.rds")), b"x").unwrap();
            let _ = std::fs::remove_file(t.overlay_root.join(format!("file{round:02}.rds")));
            round += 1;
        }
        t.vfs.acc_overlay_writes_release(fh);

        let mut lines: Vec<&str> = std::str::from_utf8(&chunked).unwrap().lines().collect();
        lines.sort();
        assert_eq!(lines.len(), 50, "every line exactly once: {lines:?}");
        for (i, line) in lines.iter().enumerate() {
            assert!(
                line.starts_with(&format!("file{i:02}.rds:")),
                "unexpected line {line:?}"
            );
        }

        // A new open sees the current state.
        assert!(read_listing(&t).contains("aaa_new0.rds:"));
    }

    #[test]
    fn released_handle_reads_nothing() {
        let root = overlay_dir("released");
        std::fs::write(root.join("a.txt"), b"x").unwrap();
        let t = make_overlay_test_vfs_with_root(MockHub::new(), MockXet::new(), root);

        let fh = t.vfs.acc_overlay_writes_open();
        assert!(!t.vfs.acc_overlay_writes_read(fh, 0, 1024).unwrap().is_empty());
        t.vfs.acc_overlay_writes_release(fh);
        assert!(t.vfs.acc_overlay_writes_read(fh, 0, 1024).unwrap().is_empty());
    }

    #[test]
    fn writes_index_is_only_visible_with_accelerator_mount() {
        let t = make_overlay_test_vfs_with_root(MockHub::new(), MockXet::new(), overlay_dir("visible"));

        with_accelerator(true, || {
            let attr = t
                .vfs
                .acc_overlay_writes_lookup(ROOT_INODE, OVERLAY_WRITES_NAME)
                .unwrap();
            assert_eq!(attr.ino, OVERLAY_WRITES_INO);
            assert_eq!(attr.perm, 0o444);
            assert!(t.vfs.acc_overlay_writes_getattr(OVERLAY_WRITES_INO).is_some());
            assert!(t.vfs.acc_overlay_writes_lookup(ROOT_INODE, "other").is_none());
        });
        with_accelerator(false, || {
            assert!(
                t.vfs
                    .acc_overlay_writes_lookup(ROOT_INODE, OVERLAY_WRITES_NAME)
                    .is_none()
            );
            assert!(t.vfs.acc_overlay_writes_getattr(OVERLAY_WRITES_INO).is_none());
        });
    }

    #[test]
    fn create_and_rewrite_take_the_caller_gid() {
        if !running_as_root() {
            eprintln!("skipping: chown to arbitrary gids needs root");
            return;
        }
        let t = make_overlay_test_vfs_with_root(MockHub::new(), MockXet::new(), overlay_dir("rewrite"));

        with_accelerator(true, || {
            let ino = t.runtime.block_on(async {
                let (attr, fh) = t
                    .vfs
                    .create(ROOT_INODE, "out.rds", 0o644, 65534, 400524, None)
                    .await
                    .unwrap();
                t.vfs.release(fh).await.unwrap();
                attr.ino
            });
            assert_eq!(
                disk_gid(&t.overlay_root, "out.rds"),
                400524,
                "create stamps the creator"
            );
            assert!(read_listing(&t).contains("out.rds:400524"));

            // A second job reopens the existing file for writing.
            t.vfs.claim_overlay_owner(ino, 65534, 731691);
            assert_eq!(
                disk_gid(&t.overlay_root, "out.rds"),
                731691,
                "rewrite stamps the last writer"
            );
            assert!(
                read_listing(&t).contains("out.rds:731691:"),
                "index reports the last writer"
            );
            // What the kernel sees is left as upstream reports it: the creator.
            let attr = t.vfs.getattr(ino).unwrap();
            assert_eq!((attr.uid, attr.gid), (65534, 400524), "kernel view is unchanged");
        });
    }

    #[test]
    fn invalid_caller_ids_are_never_stamped() {
        if !running_as_root() {
            eprintln!("skipping: chown to arbitrary gids needs root");
            return;
        }
        let t = make_overlay_test_vfs_with_root(MockHub::new(), MockXet::new(), overlay_dir("invalid_ids"));

        with_accelerator(true, || {
            let ino = t.runtime.block_on(async {
                let (attr, fh) = t
                    .vfs
                    .create(ROOT_INODE, "kept.rds", 0o644, 65534, 400524, None)
                    .await
                    .unwrap();
                t.vfs.release(fh).await.unwrap();
                attr.ino
            });
            // An idmap-capable kernel sends -1 for open/setattr callers.
            t.vfs.claim_overlay_owner(ino, u32::MAX, u32::MAX);
            t.vfs.claim_overlay_owner(ino, 65534, u32::MAX);

            assert_eq!(disk_gid(&t.overlay_root, "kept.rds"), 400524, "disk owner unchanged");
            let attr = t.vfs.getattr(ino).unwrap();
            assert_eq!((attr.uid, attr.gid), (65534, 400524), "kernel view unchanged");
        });
    }

    #[test]
    fn idmap_is_only_requested_outside_accelerator_mode() {
        with_accelerator(true, || assert!(!super::acc_allow_idmap()));
        with_accelerator(false, || assert!(super::acc_allow_idmap()));
    }

    #[test]
    fn leftover_file_is_claimed_by_rewriting_job() {
        if !running_as_root() {
            eprintln!("skipping: chown to arbitrary gids needs root");
            return;
        }
        // A leftover from an earlier run, already on disk as 0:0 before the mount starts.
        let root = overlay_dir("leftover");
        std::fs::write(root.join("old.rds"), b"stale").unwrap();
        std::os::unix::fs::chown(root.join("old.rds"), Some(0), Some(0)).unwrap();
        let t = make_overlay_test_vfs_with_root(MockHub::new(), MockXet::new(), root);

        with_accelerator(true, || {
            let ino = t.runtime.block_on(async {
                let entries = t.vfs.readdir(ROOT_INODE).await.unwrap();
                entries.iter().find(|e| e.name == "old.rds").unwrap().ino
            });
            t.vfs.claim_overlay_owner(ino, 65534, 512345);
        });
        assert_eq!(disk_gid(&t.overlay_root, "old.rds"), 512345);
    }

    #[test]
    fn ownership_is_unchanged_without_accelerator_mount() {
        if !running_as_root() {
            eprintln!("skipping: chown to arbitrary gids needs root");
            return;
        }
        let root = overlay_dir("gate_off");
        std::fs::write(root.join("plain.txt"), b"x").unwrap();
        let before = disk_gid(&root, "plain.txt");
        let t = make_overlay_test_vfs_with_root(MockHub::new(), MockXet::new(), root);

        with_accelerator(false, || {
            let ino = t.runtime.block_on(async {
                let entries = t.vfs.readdir(ROOT_INODE).await.unwrap();
                entries.iter().find(|e| e.name == "plain.txt").unwrap().ino
            });
            t.vfs.claim_overlay_owner(ino, 65534, 731691);
        });
        assert_eq!(disk_gid(&t.overlay_root, "plain.txt"), before);
    }

    #[test]
    fn symlinks_are_never_chowned() {
        if !running_as_root() {
            eprintln!("skipping: chown to arbitrary gids needs root");
            return;
        }
        let t = make_overlay_test_vfs_with_root(MockHub::new(), MockXet::new(), overlay_dir("symlink"));
        std::fs::write(t.overlay_root.join("target.txt"), b"x").unwrap();
        std::os::unix::fs::symlink("target.txt", t.overlay_root.join("link.txt")).unwrap();
        let before = disk_gid(&t.overlay_root, "target.txt");

        let overlay = t.vfs.overlay_backing.as_deref().unwrap();
        super::set_owner(overlay, "link.txt", 65534, 731691).unwrap();
        assert_eq!(disk_gid(&t.overlay_root, "target.txt"), before);
    }
}
