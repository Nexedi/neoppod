// Copyright (C) 2018  Nexedi SA and Contributors.
//                     Kirill Smelkov <kirr@nexedi.com>
//
// This program is free software: you can Use, Study, Modify and Redistribute
// it under the terms of the GNU General Public License version 3, or (at your
// option) any later version, as published by the Free Software Foundation.
//
// You can also Link and Combine this program with other software covered by
// the terms of any of the Free Software licenses or any of the Open Source
// Initiative approved licenses and Convey the resulting work. Corresponding
// source of such a combination shall include the source code for all other
// software used.
//
// This program is distributed WITHOUT ANY WARRANTY; without even the implied
// warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
//
// See COPYING file for full licensing terms.
// See https://www.nexedi.com/licensing for rationale and options.

// Package wcfs provides filesystem server with file data backed by wendelin.core arrays.
//
// Intro
//
// Each wendelin.core array (ZBigArray) is actually a linear file (ZBigFile)
// and array metadata like dtype, shape and strides associated with it. This
// package exposes as files only ZBigFile data and leaves rest of
// array-specific handling to client. Every ZBigFile is exposed as one separate
// file that represents whole ZBigFile's data.
//
// For a client, the primary way to access a bigfile should be to mmap
// bigfile/<bigfileX>/head/data which represents always latest bigfile data.
// Clients that want to get isolation guarantee should subscribe for
// invalidations and re-mmap invalidated regions to file with pinned bigfile revision for
// the duration of their transaction. See "Invalidation protocol" for details.
//
// In the usual situation when bigfiles are big, and there are O(1)/δt updates,
// there should be no need for any cache besides shared kernel cache of latest
// bigfile data.
//
//
// Filesystem organization
//
// XXX write that directories appear only after client creates <bigfileX> and @<tidX>.
//
// Top-level structure of provided filesystem is as follows:
//
//	bigfile/
//		<oid(bigfile1)>/
//			...
//		<oid(bigfile2)>/
//			...
//		...
//
// where for every bigfileX there is bigfile/<oid(bigfileX)>/ directory, with
// oid(bigfileX) being ZODB object-id of corresponding ZBigFile object formatted with %016x.
//
// Each bigfileX/ has the following structure:
//
//	bigfile/<bigfileX>/
//		head/		; latest bigfile revision
//			...
//		@<tid1>/	; bigfile revision as of transaction <tidX>
//			...
//		@<tid2>/
//			...
//		...
//
// where head/ represents latest bigfile as stored in upstream ZODB, and
// @<tidX>/ represents bigfile as of transaction <tidX>.
//
// head/ has the following structure:
//
//	bigfile/<bigfileX>/head/
//		data		; latest bigfile data
//		serial		; last update to data was at this transaction
//		invalidations	; channel that describes invalidated data regions
//
// where /data represents latest bigfile data as stored in upstream ZODB. As there
// can be some lag receiving updates from the database, /serial describes
// precisely currently exposed bigfile revision. Whenever bigfile data is changed in
// upstream ZODB, information about the changes is first propagated to
// /invalidations, and only after that /data and /serial are updated. See
// "Invalidation protocol" for details.
//
// @<tidX>/ has the following structure:
//
//	bigfile/<bigfileX>/@<tidX>/
//		data		; bigfile data as of transaction <tidX>
//
// where /data represents bigfile data as of transaction <tidX>.
//
//
// Invalidation protocol
//
// In order to support isolation wcfs implements invalidation protocol that
// must be cooperatively followed by both wcfs and client:
//
// The filesystem server itself will receive information about changed data
// from ZODB server through regular ZODB invalidation channel (as it is ZODB
// client itself). Then, before actually updating bigfile/<bigfileX>/head/data
// content in changed part, it will notify through bigfile/<bigfileX>/head/invalidations
// to clients that had opened this file (separately to each client) about the changes
//
//	XXX notification message
//
// and wait until they confirm that changed file part can be updated in global
// OS cache.
//
//	XXX client reply
//
// The clients in turn are advised to re-mmap invalidated regions to
// bigfile/<bigfileX>/@<client-tid>/data, where <client-tid> is transaction-id
// of current client view of the database.
//
// XXX protection against slow / faulty client - those that don't reply
// promptly to invalidation notification.
//
//
// Writes
//
// As each bigfile is represented by 1 synthetic file, there can be several
// write schemes:
//
// 1. mmap(MAP_PRIVATE) + writeout by client
//
// In this scheme bigfile data is mmapped in MAP_PRIVATE mode, so that local
// user changes are not automatically propagated back to the file. When there
// is a need to commit, client investigates via some OS mechanism, e.g.
// /proc/self/pagemap or something similar, which pages of this mapping it
// modified. Knowing this it knows which data it dirtied and so can write this
// data back to ZODB itself, without filesystem server providing write support.
//
// 2. write to wcfs
//
// XXX we later could implement "write-directly" mode where clients would write
// data directly into the file.
package wcfs


// Notes on OS pagecache control:
//
// the cache of snapshotted bigfile can be pre-made hot, if invalidated region
// was already in pagecache of head/data:
//
// - we can retrieve a region from pagecache of head/data with FUSE_NOTIFY_RETRIEVE.
// - we can store that retrieved data into pagecache region of @<tidX>/ with FUSE_NOTIFY_STORE.
// - we can invalidate a region from pagecache of head/data with FUSE_NOTIFY_INVAL_INODE.
//
// we have to disable FUSE_AUTO_INVAL_DATA to tell the kernel we are fully
// responsible for invalidating pagecache. If we don't, the kernel will be
// clearing whole cache of head/data on e.g. its mtime change.
//
// XXX FUSE_AUTO_INVAL_DATA does not fully prevent kernel from automatically
// invalidating pagecache - e.g. it will invalidate whole cache on file size changes:
//
// https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/tree/fs/fuse/inode.c?id=e0bc833d10#n233
//
// we can currently workaround it with using writeback mode (see !is_wb in the
// link above), but better we have proper FUSE flag for filesystem server to
// tell the kernel it is fully responsible for invalidating pagecache.


// Notes on invalidation vs faulty / slow clients:
//
// it should be possible for wcfs to stop a client with ptrace and change its
// address space in a style similar to e.g. VirtualAllocEx on windows. Hacky
// example how this could be done on Linux:
//
// https://gist.github.com/rofl0r/1073739/63f0f788a4923e26fcf743dd9a8411d4916f0ac0
//
// this way there is no way for a client to block wcfs indefinitely waiting for
// client's ack.
//
//
// However for simplicity the plan is to go first without ptrace and just kill
// a slow client on, say 30 seconds, timeout.
