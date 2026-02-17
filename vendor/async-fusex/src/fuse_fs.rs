use crate::error::AsyncFusexError;
use crate::file_system::FileSystem;
use crate::fs_util::*;
use crate::fuse_reply::{
    ReplyAttr, ReplyBMap, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry,
    ReplyLock, ReplyOpen, ReplyStatFs, ReplyWrite, ReplyXAttr,
};
#[cfg(feature = "abi-7-21")]
use crate::fuse_reply::ReplyDirectoryPlus;
use crate::fuse_request::Request;
#[cfg(feature = "abi-7-16")]
use crate::protocol::FuseForgetOne;
use crate::{VirtualFs, fs_util};
use async_trait::async_trait;
use clippy_utilities::{Cast, OverflowArithmetic};
use nix::errno::Errno;
use nix::sys::stat::SFlag;
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, instrument};

pub struct FuseFs {
    virtual_fs: Arc<dyn VirtualFs>,
}

impl FuseFs {
    pub fn new(virtual_fs: Arc<dyn VirtualFs>) -> Self {
        Self { virtual_fs }
    }
}

#[async_trait]
impl FileSystem for FuseFs {

    /// Initialize filesystem.
    /// Called before any other filesystem method.
    async fn init(&self, req: &Request<'_>) -> nix::Result<()> {
        debug!("init(req={:?}), cache size={}", req, 0_i32);
        Ok(())
    }

    /// Clean up filesystem.
    /// Called on filesystem exit.
    async fn destroy(&self, req: &Request<'_>) {
        debug!("destroy(req={:?}), cache size={}", req, 0_i32);
    }

    /// Look up a directory entry by name and get its attributes.
    async fn lookup(
        &self,
        req: &Request<'_>,
        parent: INum,
        name: &str,
        reply: ReplyEntry<'_>,
    ) -> nix::Result<usize> {
        debug!("lookup(parent={}, name={:?}, req={:?})", parent, name, req,);

        let lookup_res = self
            .virtual_fs
            .lookup(req.uid(), req.gid(), parent, name)
            .await;

        match lookup_res {
            Ok((ttl, file_attr, generation)) => {
                debug!(
                    "fusefilesystem call lookup() successfully got the attr={:?} of parent={} and name={:?}",
                    file_attr, parent, name,
                );
                let fuse_attr = fs_util::convert_to_fuse_attr(file_attr);
                reply.entry(ttl, fuse_attr, generation).await
            }
            Err(e) => {
                debug!("lookup() failed, the error is: {:?}", e);
                reply.error(e).await
            }
        }
    }

    /// Get file attributes.
    async fn getattr(&self, req: &Request<'_>, reply: ReplyAttr<'_>) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!("getattr(ino={}, req={:?})", ino, req);

        let getattr_res = self.virtual_fs.getattr(ino).await;

        match getattr_res {
            Ok((ttl, file_attr)) => {
                debug!(
                    "fusefilesystem call getattr() successfully got the attr={:?} of ino={}",
                    file_attr, ino,
                );
                let fuse_attr = fs_util::convert_to_fuse_attr(file_attr);
                reply.attr(ttl, fuse_attr).await
            }
            Err(err) => {
                // In the previous version ,this panic will never happen.
                debug!("getattr() failed to get the attr of ino={ino}, the error is: {err}",);
                reply.error(err).await
            }
        }
    }

    /// Open a file.
    /// Open flags (with the exception of `O_CREAT`, `O_EXCL`, `O_NOCTTY` and
    /// `O_TRUNC`) are available in flags. Filesystem may store an arbitrary
    /// file handle (pointer, index, etc) in fh, and use self in other all
    /// other file operations (read, write, flush, release, fsync).
    /// Filesystem may also implement stateless file I/O and not store
    /// anything in fh. There are also some flags (`direct_io`, `keep_cache`)
    /// which the filesystem may set, to change the way the file is opened.
    /// See `fuse_file_info` structure in `fuse_common.h` for more details.
    async fn open(
        &self,
        req: &Request<'_>,
        flags: u32,
        reply: ReplyOpen<'_>,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!("open(ino={}, flags={}, req={:?})", ino, flags, req);

        let open_res = self.virtual_fs.open(req.uid(), req.gid(), ino, flags).await;

        match open_res {
            Ok(fd) => {
                debug!(
                    "fusefilesystem call open() successfully opened ino={} with flags={:?}, the fd={}",
                    ino, flags, fd,
                );
                reply.opened(fd, flags).await
            }
            Err(e) => {
                debug!(
                    "fusefilesystem call open() failed to open ino={} with flags={:?}, the error is: {:?}",
                    ino, flags, e,
                );
                reply.error(e).await
            }
        }
    }

    /// Forget about an inode.
    /// The nlookup parameter indicates the number of lookups previously
    /// performed on self inode. If the filesystem implements inode
    /// lifetimes, it is recommended that inodes acquire a single reference
    /// on each lookup, and lose nlookup references on each forget. The
    /// filesystem may ignore forget calls, if the inodes don't need to have
    /// a limited lifetime. On unmount it is not guaranteed, that all referenced
    /// inodes will receive a forget message.
    #[instrument(level = "debug", skip(self))]
    async fn forget(&self, req: &Request<'_>, nlookup: u64) {
        let ino = req.nodeid();
        debug!("forget(ino={}, nlookup={}, req={:?})", ino, nlookup, req,);
        self.virtual_fs.forget(ino, nlookup).await;
    }

    /// Forget about multiple inodes.
    #[cfg(feature = "abi-7-16")]
    #[instrument(level = "debug", skip(self, nodes))]
    async fn batch_forget(&self, req: &Request<'_>, nodes: &[FuseForgetOne]) {
        debug!("batch_forget(count={}, req={:?})", nodes.len(), req);
        for node in nodes {
            self.virtual_fs.forget(node.nodeid, node.nlookup).await;
        }
    }

    /// Set file attributes.
    async fn setattr(
        &self,
        req: &Request<'_>,
        param: SetAttrParam,
        reply: ReplyAttr<'_>,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!("setattr(ino={}, param={:?}, req={:?})", ino, param, req,);

        let set_res = self
            .virtual_fs
            .setattr(req.uid(), req.gid(), ino, param)
            .await;

        match set_res {
            Ok((ttl, file_attr)) => {
                debug!(
                    "fusefilesystem call setattr() successfully set the attr={:?} of ino={}",
                    file_attr, ino,
                );
                let fuse_attr = fs_util::convert_to_fuse_attr(file_attr);
                reply.attr(ttl, fuse_attr).await
            }
            Err(e) => {
                debug!(
                    "fusefilesystem call setattr() failed to set the attr of ino={}, the error is: {:?}",
                    ino, e,
                );
                reply.error(e).await
            }
        }
    }

    /// Create file node.
    /// Create a regular file, character device, block device, fifo or socket
    /// node.
    async fn mknod(
        &self,
        req: &Request<'_>,
        param: CreateParam,
        reply: ReplyEntry<'_>,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        let name = param.name.clone();
        let mode = param.mode;
        let parent = param.parent;
        debug!(
            "mknod(ino={} parent={}, name={:?}, mode={}, req={:?})",
            ino, parent, name, mode, req,
        );

        let mknod_res = self.virtual_fs.mknod(param).await;

        match mknod_res {
            Ok((ttl, file_attr, generation)) => {
                debug!(
                    "fusefilesystem call mknod() successfully created a file name={:?} and mode={:?} under parent ino={} with attr={:?}",
                    name, mode, parent, file_attr,
                );
                let fuse_attr = fs_util::convert_to_fuse_attr(file_attr);
                reply.entry(ttl, fuse_attr, generation).await
            }
            Err(e) => {
                debug!(
                    "fusefilesystem call mknod() failed to create a file name={:?} and mode={:?} under parent ino={}, the error is: {:?}",
                    name, mode, parent, e,
                );
                reply.error(e).await
            }
        }
    }

    /// Create a directory.
    async fn mkdir(
        &self,
        req: &Request<'_>,
        parent: INum,
        name: &str,
        mode: u32,
        reply: ReplyEntry<'_>,
    ) -> nix::Result<usize> {
        debug!(
            "mkdir(parent={}, name={:?}, mode={}, req={:?})",
            parent, name, mode, req,
        );

        // TODO: this mkdir param is different from mknod one, need to unify them
        let param = CreateParam {
            parent,
            name: name.to_owned(),
            mode,
            rdev: 0,
            uid: req.uid(),
            gid: req.gid(),
            node_type: SFlag::S_IFDIR,
            link: None,
        };

        let mkdir_res = self.virtual_fs.mkdir(param).await;

        match mkdir_res {
            Ok((ttl, file_attr, generation)) => {
                debug!(
                    "fusefilesystem call mkdir() successfully created a directory name={:?} and mode={:?} under parent ino={} with attr={:?}",
                    name, mode, parent, file_attr,
                );
                let fuse_attr = fs_util::convert_to_fuse_attr(file_attr);
                reply.entry(ttl, fuse_attr, generation).await
            }
            Err(e) => {
                debug!(
                    "fusefilesystem call mkdir() failed to create a directory name={:?} and mode={:?} under parent ino={}, \
                        the error is: {:?}",
                    name, mode, parent, e,
                );
                reply.error(e).await
            }
        }
    }

    /// Remove a file.
    #[instrument(level = "debug", skip(self), err, ret)]
    async fn unlink(
        &self,
        req: &Request<'_>,
        parent: INum,
        name: &str,
        reply: ReplyEmpty<'_>,
    ) -> nix::Result<usize> {
        debug!("unlink(parent={}, name={:?}, req={:?}", parent, name, req,);

        let unlink_res = self
            .virtual_fs
            .unlink(req.uid(), req.gid(), parent, name)
            .await;

        match unlink_res {
            Ok(()) => {
                debug!(
                    "fusefilesystem call unlink() successfully removed a file name={:?} under parent ino={}",
                    name, parent,
                );
                reply.ok().await
            }
            Err(e) => {
                debug!(
                    "fusefilesystem call unlink() failed to remove a file name={:?} under parent ino={}, the error is: {:?}",
                    name, parent, e,
                );
                reply.error(e).await
            }
        }
    }

    /// Remove a directory.
    #[instrument(level = "debug", skip(self), err, ret)]
    async fn rmdir(
        &self,
        req: &Request<'_>,
        parent: INum,
        dir_name: &str,
        reply: ReplyEmpty<'_>,
    ) -> nix::Result<usize> {
        let rmdir_res = self
            .virtual_fs
            .rmdir(req.uid(), req.gid(), parent, dir_name)
            .await;

        match rmdir_res {
            Ok(_) => {
                debug!(
                    "fusefilesystem call rmdir() successfully removed a directory name={:?} under parent ino={}",
                    dir_name, parent,
                );
                reply.ok().await
            }
            Err(e) => {
                debug!(
                    "fusefilesystem call rmdir() failed to remove a directory name={:?} under parent ino={}, the error is: {:?}",
                    dir_name, parent, e,
                );
                reply.error(e).await
            }
        }
    }

    /// Rename a file
    ///
    /// If the target exists it should be atomically replaced. If
    /// the target's inode's lookup count is non-zero, the file
    /// system is expected to postpone any removal of the inode
    /// until the lookup count reaches zero (see description of the
    /// forget function).
    ///
    /// *flags* may be `RENAME_EXCHANGE` or `RENAME_NOREPLACE`. If
    /// `RENAME_NOREPLACE` is specified, the filesystem must not
    /// overwrite *newname* if it exists and return an error
    /// instead. If `RENAME_EXCHANGE` is specified, the filesystem
    /// must atomically exchange the two files, i.e. both must
    /// exist and neither may be deleted.
    async fn rename(
        &self,
        req: &Request<'_>,
        param: RenameParam,
        reply: ReplyEmpty<'_>,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        let old_parent = param.old_parent;
        let old_name = param.old_name.clone();
        let new_parent = param.new_parent;
        let new_name = param.new_name.clone();
        debug!(
            "rename(ino={} oldparent={}, oldname={:?}, newparent={}, newname={:?}, req={:?})",
            ino, old_parent, old_name, new_parent, new_name, req,
        );

        let rename_res = self.virtual_fs.rename(req.uid(), req.gid(), param).await;

        match rename_res {
            Ok(()) => {
                debug!(
                    "fusefilesystem call rename() successfully renamed the file name={:?} under parent ino={}",
                    old_name, old_parent,
                );
                reply.ok().await
            }
            Err(e) => {
                debug!(
                    "fusefilesystem call rename() failed to rename the file name={:?} under parent ino={}, the error is: {:?}",
                    old_name, old_parent, e,
                );
                reply.error(e).await
            }
        }
    }

    /// Read data.
    /// Read should send exactly the number of bytes requested except on EOF or
    /// error, otherwise the rest of the data will be substituted with
    /// zeroes. An exception to self is when the file has been opened in
    /// `direct_io` mode, in which case the return value of the read system
    /// call will reflect the return value of self operation. fh will
    /// contain the value set by the open method, or will be undefined
    /// if the open method didn't set any value.
    async fn read(
        &self,
        req: &Request<'_>,
        fh: u64,
        offset: i64,
        size: u32,
        reply: ReplyData<'_>,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        let offset: u64 = offset.cast();
        debug!(
            "read(ino={}, fh={}, offset={}, size={}, req={:?})",
            ino, fh, offset, size, req,
        );

        // Try to use the buffer size as the size of the read buffer
        let mut buf = Vec::new();

        let read_res = self.virtual_fs.read(ino, offset, size, &mut buf).await;

        // Check the load result
        match read_res {
            Ok(content_size) => {
                debug!(
                    "fusefilesystem call read() successfully read the content of ino={} with size={}",
                    ino, content_size,
                );
                reply.data(buf.clone()).await
            }
            Err(e) => {
                debug!(
                    "fusefilesystem call read() failed to read the content of ino={}, the error is: {:?}",
                    ino, e,
                );
                reply.error(e).await
            }
        }
    }

    /// Write data.
    /// Write should return exactly the number of bytes requested except on
    /// error. An exception to self is when the file has been opened in
    /// `direct_io` mode, in which case the return value of the write system
    /// call will reflect the return value of self operation. fh will
    /// contain the value set by the open method, or will be undefined if
    /// the open method did not set any value.
    #[instrument(level = "debug", skip(self, data, req), err, ret)]
    async fn write(
        &self,
        req: &Request<'_>,
        fh: u64,
        offset: i64,
        data: &[u8],
        flags: u32,
        reply: ReplyWrite<'_>,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        let data_len: u64 = data.len().cast();
        debug!(
            "write(ino={}, fh={}, offset={}, data_len={}, req={:?})",
            ino, fh, offset, data_len, req,
        );

        let write_result = self.virtual_fs.write(ino, offset, data, flags).await;

        match write_result {
            Ok(()) => {
                debug!(
                    "fusefilesystem call write() successfully wrote the content of ino={} with size={}",
                    ino, data_len,
                );
                reply.written(data_len.cast()).await
            }
            Err(e) => {
                debug!(
                    "fusefilesystem call write() failed to write the content of ino={} with size={}, the error is: {:?}",
                    ino, data_len, e,
                );
                reply.error(e).await
            }
        }
    }

    /// Copy a range of bytes between two files.
    #[allow(clippy::too_many_arguments)]
    async fn copy_file_range(
        &self,
        req: &Request<'_>,
        _fh_in: u64,
        offset_in: i64,
        nodeid_out: u64,
        _fh_out: u64,
        offset_out: i64,
        len: u64,
        flags: u64,
        reply: ReplyWrite<'_>,
    ) -> nix::Result<usize> {
        let nodeid_in = req.nodeid();
        debug!(
            "copy_file_range(nodeid_in={}, nodeid_out={}, offset_in={}, offset_out={}, len={}, flags={:#x}, req={:?})",
            nodeid_in, nodeid_out, offset_in, offset_out, len, flags, req,
        );

        if flags != 0 {
            return reply.error_code(Errno::EINVAL).await;
        }

        if len == 0 {
            return reply.written(0).await;
        }

        let mut copied: u64 = 0;
        while copied < len {
            let remaining = len.overflow_sub(copied);
            let chunk_size = remaining.min(128 * 1024).cast::<u32>();
            let mut buf = Vec::new();
            let in_offset = offset_in.overflow_add(copied.cast::<i64>()).cast::<u64>();
            let out_offset = offset_out.overflow_add(copied.cast::<i64>());

            let read_size = match self
                .virtual_fs
                .read(nodeid_in, in_offset, chunk_size, &mut buf)
                .await
            {
                Ok(read_size) => read_size,
                Err(e) => return reply.error(e).await,
            };

            if read_size == 0 {
                break;
            }

            let payload = &buf[..read_size.min(buf.len())];
            if let Err(e) = self.virtual_fs.write(nodeid_out, out_offset, payload, 0).await {
                return reply.error(e).await;
            }

            copied = copied.overflow_add(payload.len().cast());
            if payload.len() < chunk_size.cast::<usize>() {
                break;
            }
        }

        if copied > u32::MAX.cast::<u64>() {
            return reply.error_code(Errno::EOVERFLOW).await;
        }

        reply.written(copied.cast()).await
    }

    /// Flush method.
    /// This is called on each close() of the opened file. Since file
    /// descriptors can be duplicated (dup, dup2, fork), for one open call
    /// there may be many flush calls. Filesystems should not assume that
    /// flush will always be called after some writes, or that if will be
    /// called at all. fh will contain the value set by the open method, or
    /// will be undefined if the open method did not set any value.
    /// NOTE: the name of the method is misleading, since (unlike fsync) the
    /// filesystem is not forced to flush pending writes. One reason to
    /// flush data, is if the filesystem wants to return write errors. If
    /// the filesystem supports file locking operations (setlk, getlk) it
    /// should remove all locks belonging to `lock_owner`.
    #[instrument(level = "debug", skip(self), err, ret)]
    async fn flush(
        &self,
        req: &Request<'_>,
        fh: u64,
        lock_owner: u64,
        reply: ReplyEmpty<'_>,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!(
            "flush(ino={}, fh={}, lock_owner={}, req={:?})",
            ino, fh, lock_owner, req,
        );

        // This is called from every close on an open file, so call the
        // close on the underlying filesystem.	But since flush may be
        // called multiple times for an open file, self must not really
        // close the file. This is important if used on a network
        // filesystem like NFS which flush the data/metadata on close()
        let flush_res = self.virtual_fs.flush(ino, lock_owner).await;

        match flush_res {
            Ok(()) => {
                debug!(
                    "fusefilesystem call flush() successfully flushed the content of ino={} with fh={}",
                    ino, fh,
                );
                reply.ok().await
            }
            Err(e) => {
                debug!(
                    "fusefilesystem call flush() failed to flush the content of ino={} with fh={}, the error is: {:?}",
                    ino, fh, e,
                );
                reply.error(e).await
            }
        }
    }

    /// Release an open file.
    /// Release is called when there are no more references to an open file: all
    /// file descriptors are closed and all memory mappings are unmapped.
    /// For every open call there will be exactly one release call. The
    /// filesystem may reply with an error, but error values are not
    /// returned to close() or munmap() which triggered the release. fh will
    /// contain the value set by the open method, or will be undefined
    /// if the open method didn't set any value. flags will contain the same
    /// flags as for open.
    #[instrument(level = "debug", skip(self), err, ret)]
    async fn release(
        &self,
        req: &Request<'_>,
        fh: u64,
        flags: u32, // same as the open flags
        lock_owner: u64,
        flush: bool,
        reply: ReplyEmpty<'_>,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!(
            "release(ino={}, fh={}, flags={}, lock_owner={}, flush={}, req={:?})",
            ino, fh, flags, lock_owner, flush, req,
        );

        let release_res = self.virtual_fs.release(ino, flags, lock_owner, flush).await;

        match release_res {
            Ok(()) => {
                debug!(
                    "fusefilesystem call release() successfully released the content of ino={} with fh={}",
                    ino, fh,
                );
                reply.ok().await
            }
            Err(e) => {
                debug!(
                    "fusefilesystem call release() failed to release the content of ino={} with fh={}, the error is: {:?}",
                    ino, fh, e,
                );
                reply.error(e).await
            }
        }
    }

    /// Synchronize file contents.
    /// If the datasync parameter is non-zero, then only the user data should be
    /// flushed, not the meta data.
    #[instrument(level = "debug", skip(self), err, ret)]
    async fn fsync(
        &self,
        req: &Request<'_>,
        fh: u64,
        datasync: bool,
        reply: ReplyEmpty<'_>,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!("fsync(ino={}, fh={}, req={:?})", ino, fh, req,);

        let fsync_res = self.virtual_fs.fsync(ino, datasync).await;

        match fsync_res {
            Ok(()) => {
                debug!(
                    "fusefilesystem call fsync() successfully flushed the content of ino={} with fh={}",
                    ino, fh,
                );
                reply.ok().await
            }
            Err(e) => {
                debug!(
                    "fusefilesystem call fsync() failed to flush the content of ino={} with fh={}, the error is: {:?}",
                    ino, fh, e,
                );
                reply.error(e).await
            }
        }
    }

    /// Open a directory.
    /// Filesystem may store an arbitrary file handle (pointer, index, etc) in
    /// fh, and use self in other all other directory stream operations
    /// (readdir, releasedir, fsyncdir). Filesystem may also implement
    /// stateless directory I/O and not store anything in fh, though that
    /// makes it impossible to implement standard conforming
    /// directory stream operations in case the contents of the directory can
    /// change between opendir and releasedir.
    async fn opendir(
        &self,
        req: &Request<'_>,
        flags: u32,
        reply: ReplyOpen<'_>,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!("opendir(ino={}, flags={}, req={:?})", ino, flags, req,);

        let opendir_res = self
            .virtual_fs
            .opendir(req.uid(), req.gid(), ino, flags)
            .await;

        match opendir_res {
            Ok(new_fd) => {
                debug!(
                    "fusefilesystem call opendir() successfully duplicated the file handler of ino={} with flags={:?}",
                    ino, flags,
                );
                reply.opened(new_fd, flags).await
            }
            Err(e) => {
                debug!(
                    "fusefilesystem call opendir() failed to duplicate the file handler of ino={} with flags={:?}, the error is: {:?}",
                    ino, flags, e,
                );
                reply.error(e).await
            }
        }
    }

    /// Read directory.
    /// Send a buffer filled using buffer.fill(), with size not exceeding the
    /// requested size. Send an empty buffer on end of stream. fh will contain
    /// the value set by the opendir method, or will be undefined if the
    /// opendir method didn't set any value.
    async fn readdir(
        &self,
        req: &Request<'_>,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory<'_>,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!(
            "readdir(ino={}, fh={}, offset={}, req={:?})",
            ino, fh, offset, req,
        );

        let readdir_res = self
            .virtual_fs
            .readdir(req.uid(), req.gid(), ino, fh, offset)
            .await;

        match readdir_res {
            Ok(dir_entries) => {
                for (i, dir_etnry) in dir_entries.iter().enumerate().skip(offset.cast()) {
                    reply.add(
                        dir_etnry.ino(),
                        offset.overflow_add(i.cast()).overflow_add(1), /* i + 1 means the index of
                                                                        * the next entry */
                        dir_etnry.file_type().into(),
                        dir_etnry.name(),
                    );
                }

                debug!(
                    "fusefilesystem call readdir() successfully read the content of ino={} with fh={}",
                    ino, fh,
                );
                reply.ok().await
            }
            Err(e) => {
                debug!(
                    "fusefilesystem call readdir() failed to read the content of ino={} with fh={}, the error is: {:?}",
                    ino, fh, e,
                );
                reply.error(e).await
            }
        }
    }

    /// Read directory plus attributes.
    #[cfg(feature = "abi-7-21")]
    async fn readdirplus(
        &self,
        req: &Request<'_>,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectoryPlus<'_>,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!(
            "readdirplus(ino={}, fh={}, offset={}, req={:?})",
            ino, fh, offset, req,
        );

        let readdir_res = self
            .virtual_fs
            .readdir(req.uid(), req.gid(), ino, fh, offset)
            .await;

        match readdir_res {
            Ok(dir_entries) => {
                for (i, dir_entry) in dir_entries.iter().enumerate().skip(offset.cast()) {
                    let resolved = match self
                        .virtual_fs
                        .lookup(req.uid(), req.gid(), ino, dir_entry.name())
                        .await
                    {
                        Ok((ttl, file_attr, generation)) => {
                            Ok((ttl, fs_util::convert_to_fuse_attr(file_attr), generation))
                        }
                        Err(lookup_err) => {
                            debug!(
                                "readdirplus lookup fallback for ino={} name={:?}, error={:?}",
                                ino,
                                dir_entry.name(),
                                lookup_err,
                            );
                            self.virtual_fs
                                .getattr(dir_entry.ino())
                                .await
                                .map(|(ttl, file_attr)| {
                                    (ttl, fs_util::convert_to_fuse_attr(file_attr), 0)
                                })
                        }
                    };

                    if let Ok((ttl, fuse_attr, generation)) = resolved {
                        let full = reply.add(
                            dir_entry.ino(),
                            offset.overflow_add(i.cast()).overflow_add(1),
                            dir_entry.file_type().into(),
                            dir_entry.name(),
                            ttl,
                            fuse_attr,
                            generation,
                        );
                        if full {
                            break;
                        }
                    }
                }

                debug!(
                    "fusefilesystem call readdirplus() successfully read the content of ino={} with fh={}",
                    ino, fh,
                );
                reply.ok().await
            }
            Err(e) => {
                debug!(
                    "fusefilesystem call readdirplus() failed to read the content of ino={} with fh={}, the error is: {:?}",
                    ino, fh, e,
                );
                reply.error(e).await
            }
        }
    }

    /// Release an open directory.
    /// For every opendir call there will be exactly one releasedir call. fh
    /// will contain the value set by the opendir method, or will be
    /// undefined if the opendir method didn't set any value.
    async fn releasedir(
        &self,
        req: &Request<'_>,
        fh: u64,
        flags: u32,
        reply: ReplyEmpty<'_>,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!(
            "releasedir(ino={}, fh={}, flags={}, req={:?})",
            ino, fh, flags, req,
        );

        // TODO: handle flags
        let releasedir_res = self.virtual_fs.releasedir(ino, fh, flags).await;

        match releasedir_res {
            Ok(()) => {
                debug!(
                    "fusefilesystem call releasedir() successfully released the content of ino={} with fh={}",
                    ino, fh,
                );
                reply.ok().await
            }
            Err(e) => {
                debug!(
                    "fusefilesystem call releasedir() failed to release the content of ino={} with fh={}, the error is: {:?}",
                    ino, fh, e,
                );
                reply.error(e).await
            }
        }
    }

    /// Synchronize directory contents.
    /// If the datasync parameter is set, then only the directory contents
    /// should be flushed, not the meta data. fh will contain the value set
    /// by the opendir method, or will be undefined if the opendir method
    /// didn't set any value.
    #[instrument(level = "debug", skip(self), err, ret)]
    async fn fsyncdir(
        &self,
        req: &Request<'_>,
        fh: u64,
        datasync: bool,
        reply: ReplyEmpty<'_>,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!(
            "fsyncdir(ino={}, fh={}, datasync={}, req={:?})",
            ino, fh, datasync, req,
        );

        let fsyncdir_res = self.virtual_fs.fsyncdir(ino, fh, datasync).await;

        match fsyncdir_res {
            Ok(()) => {
                debug!(
                    "fusefilesystem call fsyncdir() successfully flushed the content of ino={} with fh={}",
                    ino, fh,
                );
            }
            Err(e) => {
                debug!(
                    "fusefilesystem call fsyncdir() failed to flush the content of ino={} with fh={}, the error is: {:?}",
                    ino, fh, e,
                );
            }
        }

        // Similarity to rmdir, we don't store dir information in the persistent
        // storage, so we don't need to flush it
        reply.ok().await
    }

    /// Get file system statistics.
    /// The `f_favail`, `f_fsid` and `f_flag` fields are ignored
    async fn statfs(&self, req: &Request<'_>, reply: ReplyStatFs<'_>) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!("statfs(ino={}, req={:?})", ino, req,);

        let statfs_res = self.virtual_fs.statfs(req.uid(), req.gid(), ino).await;

        match statfs_res {
            Ok(statvfs) => {
                debug!(
                    "fusefilesystem call statfs() successfully read the statvfs of ino={}",
                    ino,
                );
                reply.statfs(statvfs).await
            }
            Err(e) => {
                debug!(
                    "fusefilesystem call statfs() failed to read the statvfs of ino={}, the error is: {:?}",
                    ino, e,
                );
                reply.error(e).await
            }
        }
    }

    /// Read symbolic link.
    async fn readlink(&self, req: &Request<'_>, reply: ReplyData<'_>) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!("readlink(ino={}, req={:?})", ino, req,);

        let readlink_res = self.virtual_fs.readlink(ino).await;

        match readlink_res {
            Ok(target_path) => {
                debug!(
                    "fusefilesystem call readlink() successfully read the target path of ino={}",
                    ino,
                );
                reply.data(target_path).await
            }
            Err(e) => {
                debug!(
                    "fusefilesystem call readlink() failed to read the target path of ino={}, the error is: {:?}",
                    ino, e,
                );
                reply.error(e).await
            }
        }
    }

    /// Create a symbolic link.
    async fn symlink(
        &self,
        req: &Request<'_>,
        parent: INum,
        name: &str,
        target_path: &Path,
        reply: ReplyEntry<'_>,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!(
            "symlink(ino={} parent={}, name={:?}, target_path={:?}, req={:?})",
            ino, parent, name, target_path, req,
        );

        let symlink_res = self
            .virtual_fs
            .symlink(req.uid(), req.gid(), parent, name, target_path)
            .await;

        match symlink_res {
            Ok((ttl, file_attr, generation)) => {
                debug!(
                    "fusefilesystem call symlink() successfully created a symlink name={:?} to target path={:?} under parent ino={}",
                    name, target_path, parent,
                );
                let fuse_attr = fs_util::convert_to_fuse_attr(file_attr);
                reply.entry(ttl, fuse_attr, generation).await
            }
            Err(e) => {
                debug!(
                    "fusefilesystem call symlink() failed to create a symlink name={:?} to target path={:?} under parent ino={}, \
                        the error is: {:?}",
                    name, target_path, parent, e,
                );
                reply.error(e).await
            }
        }
    }

    // Un-implemented FUSE operations

    /// Interrupt another FUSE request
    async fn interrupt(&self, req: &Request<'_>, unique: u64) {
        debug!("interrupt(req={:?}, unique={})", req, unique);

        let interrupt_res = self.virtual_fs.interrupt(unique).await;

        debug!(
            "fusefilesystem call interrupt() with unique={} {:?}",
            unique, interrupt_res,
        );
    }

    /// Create a hard link.
    async fn link(
        &self,
        req: &Request<'_>,
        old_ino: u64,
        newname: &str,
        reply: ReplyEntry<'_>,
    ) -> nix::Result<usize> {
        let newparent = req.nodeid();
        debug!(
            "link(old_ino={}, newparent={}, newname={:?}, req={:?})",
            old_ino, newparent, newname, req,
        );

        let link_res = self.virtual_fs.link(old_ino, newparent, newname).await;

        match link_res {
            Ok((ttl, file_attr, generation)) => {
                debug!(
                    "fusefilesystem call link() successfully created a hard link old_ino={}, name={:?} under parent ino={}",
                    old_ino, newname, newparent,
                );
                let fuse_attr = fs_util::convert_to_fuse_attr(file_attr);
                reply.entry(ttl, fuse_attr, generation).await
            }
            Err(AsyncFusexError::Unimplemented { context }) => {
                debug!(
                    "fusefilesystem call link() failed to create a hard link name={:?} under parent ino={}, the error is: {:?}",
                    newname, newparent, context,
                );
                reply.error_code(Errno::ENOSYS).await
            }
            Err(e) => {
                debug!(
                    "fusefilesystem call link() failed to create a hard link name={:?} under parent ino={}, the error is: {:?}",
                    newname, newparent, e,
                );
                reply.error(e).await
            }
        }
    }

    /// Set an extended attribute.
    async fn setxattr(
        &self,
        req: &Request<'_>,
        name: &str,
        value: &[u8],
        flags: u32,
        position: u32,
        reply: ReplyEmpty<'_>,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!(
            "setxattr(ino={}, name={:?}, value={:?}, flags={}, position={}, req={:?})",
            ino, name, value, flags, position, req,
        );

        let setxattr_res = self
            .virtual_fs
            .setxattr(ino, name, value, flags, position)
            .await;

        match setxattr_res {
            Ok(()) => {
                debug!(
                    "fusefilesystem call setxattr() successfully set the extended attribute name={:?}",
                    name,
                );
                reply.ok().await
            }
            Err(AsyncFusexError::Unimplemented { context }) => {
                debug!(
                    "fusefilesystem call setxattr() failed to set the extended attribute name={:?}, the error is: {:?}",
                    name, context,
                );
                reply.error_code(Errno::ENOSYS).await
            }
            Err(e) => {
                debug!(
                    "fusefilesystem call setxattr() failed to set the extended attribute name={:?}, the error is: {:?}",
                    name, e,
                );
                reply.error(e).await
            }
        }
    }

    /// Get an extended attribute.
    /// If `size` is 0, the size of the value should be sent with
    /// `reply.size()`. If `size` is not 0, and the value fits, send it with
    /// `reply.data()`, or `reply.error(ERANGE)` if it doesn't.
    async fn getxattr(
        &self,
        req: &Request<'_>,
        name: &str,
        size: u32,
        reply: ReplyXAttr<'_>,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!(
            "getxattr(ino={}, name={:?}, size={}, req={:?})",
            ino, name, size, req,
        );

        let getxattr_res = self.virtual_fs.getxattr(ino, name, size).await;

        match getxattr_res {
            Ok(value) => {
                debug!(
                    "fusefilesystem call getxattr() successfully get the extended attribute name={:?}",
                    name,
                );
                if size == 0 {
                    reply.size(value.len() as u32).await
                } else {
                    reply.data(value).await
                }
            }
            Err(AsyncFusexError::Unimplemented { context }) => {
                debug!(
                    "fusefilesystem call getxattr() failed to get the extended attribute name={:?}, the error is: {:?}",
                    name, context,
                );
                reply.error_code(Errno::ENOSYS).await
            }
            Err(e) => {
                debug!(
                    "fusefilesystem call getxattr() failed to get the extended attribute name={:?}, the error is: {:?}",
                    name, e,
                );
                reply.error(e).await
            }
        }
    }

    /// List extended attribute names.
    /// If `size` is 0, the size of the value should be sent with
    /// `reply.size()`. If `size` is not 0, and the value fits, send it with
    /// `reply.data()`, or `reply.error(ERANGE)` if it doesn't.
    async fn listxattr(
        &self,
        req: &Request<'_>,
        size: u32,
        reply: ReplyXAttr<'_>,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!("listxattr(ino={}, size={}, req={:?})", ino, size, req,);

        let listxattr_res = self.virtual_fs.listxattr(ino, size).await;

        match listxattr_res {
            Ok(value) => {
                debug!(
                    "fusefilesystem call listxattr() successfully list the extended attribute names",
                );
                if size == 0 {
                    reply.size(value.len() as u32).await
                } else {
                    reply.data(value).await
                }
            }
            Err(AsyncFusexError::Unimplemented { context }) => {
                debug!(
                    "fusefilesystem call listxattr() failed to list the extended attribute names, the error is: {:?}",
                    context,
                );
                reply.error_code(Errno::ENOSYS).await
            }
            Err(e) => {
                debug!(
                    "fusefilesystem call listxattr() failed to list the extended attribute names, the error is: {:?}",
                    e,
                );
                reply.error(e).await
            }
        }
    }

    /// Remove an extended attribute.
    async fn removexattr(
        &self,
        req: &Request<'_>,
        name: &str,
        reply: ReplyEmpty<'_>,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!("removexattr(ino={}, name={:?}, req={:?})", ino, name, req,);

        let removexattr_res = self.virtual_fs.removexattr(ino, name).await;

        match removexattr_res {
            Ok(()) => {
                debug!(
                    "fusefilesystem call removexattr() successfully removed the extended attribute name={:?}",
                    name,
                );
                reply.ok().await
            }
            Err(AsyncFusexError::Unimplemented { context }) => {
                debug!(
                    "fusefilesystem call removexattr() failed to remove the extended attribute name={:?}, the error is: {:?}",
                    name, context,
                );
                reply.error_code(Errno::ENOSYS).await
            }
            Err(e) => {
                debug!(
                    "fusefilesystem call removexattr() failed to remove the extended attribute name={:?}, the error is: {:?}",
                    name, e,
                );
                reply.error(e).await
            }
        }
    }

    /// Check file access permissions.
    /// This will be called for the `access()` system call. If the
    /// `default_permissions` mount option is given, self method is not
    /// called. This method is not called under Linux kernel versions 2.4.x
    async fn access(
        &self,
        req: &Request<'_>,
        mask: u32,
        reply: ReplyEmpty<'_>,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!("access(ino={}, mask={}, req={:?})", ino, mask, req,);

        let access_res = self
            .virtual_fs
            .access(req.uid(), req.gid(), ino, mask)
            .await;

        match access_res {
            Ok(()) => {
                debug!(
                    "fusefilesystem call access() successfully checked the access permission with mask={}",
                    mask,
                );
                reply.ok().await
            }
            Err(AsyncFusexError::Unimplemented { context }) => {
                debug!(
                    "fusefilesystem call access() failed to check the access permission with mask={}, the error is: {:?}",
                    mask, context,
                );
                reply.error_code(Errno::ENOSYS).await
            }
            Err(e) => {
                debug!(
                    "fusefilesystem call access() failed to check the access permission with mask={}, the error is: {:?}",
                    mask, e,
                );
                reply.error(e).await
            }
        }
    }

    /// Create and open a file.
    /// If the file does not exist, first create it with the specified mode, and
    /// then open it. Open flags (with the exception of `O_NOCTTY`) are
    /// available in flags. Filesystem may store an arbitrary file handle
    /// (pointer, index, etc) in fh, and use self in other all other file
    /// operations (read, write, flush, release, fsync). There are also some
    /// flags (`direct_io`, `keep_cache`) which the filesystem may set, to
    /// change the way the file is opened. See `fuse_file_info` structure in
    /// `fuse_common.h` for more details. If self method is not implemented
    /// or under Linux kernel versions earlier than 2.6.15, the mknod()
    /// and open() methods will be called instead.
    async fn create(
        &self,
        req: &Request<'_>,
        parent: u64,
        name: &str,
        mode: u32,
        flags: u32,
        reply: ReplyCreate<'_>,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!(
            "create(ino={}, parent={}, name={:?}, mode={}, flags={}, req={:?})",
            ino, parent, name, mode, flags, req,
        );

        let create_res = self
            .virtual_fs
            .create(req.uid(), req.gid(), ino, parent, name, mode, flags)
            .await;

        match create_res {
            Ok((ttl, file_attr, generation, fh, open_flags)) => {
                debug!(
                    "fusefilesystem call create() successfully created a file name={:?} under parent ino={}",
                    name, parent,
                );
                let fuse_attr = fs_util::convert_to_fuse_attr(file_attr);
                reply.created(&ttl, fuse_attr, generation, fh, open_flags).await
            }
            Err(AsyncFusexError::Unimplemented { context }) => {
                debug!(
                    "fusefilesystem call create() failed to create a file name={:?} under parent ino={}, the error is: {:?}",
                    name, parent, context,
                );
                reply.error_code(Errno::ENOSYS).await
            }
            Err(e) => {
                debug!(
                    "fusefilesystem call create() failed to create a file name={:?} under parent ino={}, the error is: {:?}",
                    name, parent, e,
                );
                reply.error(e).await
            }
        }
    }

    /// Test for a POSIX file lock.
    async fn getlk(
        &self,
        req: &Request<'_>,
        lk_param: FileLockParam,
        reply: ReplyLock<'_>,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!("getlk(ino={}, lk_param={:?}, req={:?})", ino, lk_param, req,);

        let getlk_res = self
            .virtual_fs
            .getlk(req.uid(), req.gid(), ino, lk_param)
            .await;

        match getlk_res {
            Ok(lock) => {
                debug!(
                    "fusefilesystem call getlk() successfully get the file lock of ino={}",
                    ino,
                );
                reply.locked(lock.start, lock.end, lock.typ, lock.pid).await
            }
            Err(AsyncFusexError::Unimplemented { context }) => {
                debug!(
                    "fusefilesystem call getlk() failed to get the file lock of ino={}, the error is: {:?}",
                    ino, context,
                );
                reply.error_code(Errno::ENOSYS).await
            }
            Err(e) => {
                debug!(
                    "fusefilesystem call getlk() failed to get the file lock of ino={}, the error is: {:?}",
                    ino, e,
                );
                reply.error(e).await
            }
        }
    }

    /// Acquire, modify or release a POSIX file lock.
    /// For POSIX threads (NPTL) there's a 1-1 relation between pid and owner,
    /// but otherwise self is not always the case.  For checking lock
    /// ownership, `fi->owner` must be used. The `l_pid` field in `struct
    /// flock` should only be used to fill in self field in `getlk()`. Note:
    /// if the locking methods are not implemented, the kernel will still
    /// allow file locking to work locally. Hence these are only interesting
    /// for network filesystems and similar.
    async fn setlk(
        &self,
        req: &Request<'_>,
        lk_param: FileLockParam,
        sleep: bool,
        reply: ReplyEmpty<'_>,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!(
            "setlk(ino={}, lk_param={:?}, sleep={}, req={:?})",
            ino, lk_param, sleep, req,
        );

        let setlk_res = self
            .virtual_fs
            .setlk(req.uid(), req.gid(), ino, lk_param, sleep)
            .await;

        match setlk_res {
            Ok(()) => {
                debug!(
                    "fusefilesystem call setlk() successfully set the file lock of ino={}",
                    ino,
                );
                reply.ok().await
            }
            Err(AsyncFusexError::Unimplemented { context }) => {
                debug!(
                    "fusefilesystem call setlk() failed to set the file lock of ino={}, the error is: {:?}",
                    ino, context,
                );
                reply.error_code(Errno::ENOSYS).await
            }
            Err(e) => {
                debug!(
                    "fusefilesystem call setlk() failed to set the file lock of ino={}, the error is: {:?}",
                    ino, e,
                );
                reply.error(e).await
            }
        }
    }

    /// Map block index within file to block index within device.
    /// Note: This makes sense only for block device backed filesystems mounted
    /// with the `blkdev` option
    async fn bmap(
        &self,
        req: &Request<'_>,
        blocksize: u32,
        idx: u64,
        reply: ReplyBMap<'_>,
    ) -> nix::Result<usize> {
        let ino = req.nodeid();
        debug!(
            "bmap(ino={}, blocksize={}, idx={}, req={:?})",
            ino, blocksize, idx, req,
        );

        let bmap_res = self
            .virtual_fs
            .bmap(req.uid(), req.gid(), ino, blocksize, idx)
            .await;

        match bmap_res {
            Ok(()) => {
                debug!(
                    "fusefilesystem call bmap() successfully mapped the block index of ino={} to block index of device",
                    ino,
                );
                // TODO: reply the block
                // reply.bmap(block).await
                reply.error_code(Errno::ENOSYS).await
            }
            Err(AsyncFusexError::Unimplemented { context }) => {
                debug!(
                    "fusefilesystem call bmap() failed to map the block index of ino={} to block index of device, the error is: {:?}",
                    ino, context,
                );
                reply.error_code(Errno::ENOSYS).await
            }
            Err(e) => {
                debug!(
                    "fusefilesystem call bmap() failed to map the block index of ino={} to block index of device, the error is: {:?}",
                    ino, e,
                );
                reply.error(e).await
            }
        }
    }
}
