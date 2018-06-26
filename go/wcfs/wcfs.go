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
// Top-level structure of provided filesystem is as follows:
//
//	bigfile/
//		<oid(bigfile1)>/
//			...
//		<oid(bigfile2)>/
//			...
//		...
//
// where for a bigfileX there is bigfile/<oid(bigfileX)>/ directory, with
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
// bigfile/<bigfileX>/ should be created by client via mkdir. Unless explicitly
// created bigfile/<bigfileX>/ are not automatically visible in wcfs
// filesystem. Similarly bigfile/<bigfileX>/@<tidX>/ should be too created by
// client.
//
//
// Invalidation protocol
//
// In order to support isolation wcfs implements invalidation protocol that
// must be cooperatively followed by both wcfs and client.
//
// First, before client wants to mmap bigfile, it opens
// bigfile/<bigfileX>/head/invalidations and tells wcfs through it for which
// ZODB state it wants to get bigfile view. The server in turn reports for
// which ZODB state head/data is current, δ describing changed bigfile region
// between those revisions, or "wait" flag if server state is earlier compared
// to what client wants:
//
//	C: want <Cat>
//	S: have <Sat>, wait		; Sat < Cat
//	S: have <Sat>, δR(Cat,Sat)	; Sat ≥ Cat
//
// If server reply was "wait" the client does nothing and waits for next server
// message which must come without "wait" flag set. When client receives have
// message with δR(Cat,Sat) it has the guarantee from wcfs that head/data
// content is for Sat ZODB revision and won't change until client sends ack
// back to the server. The client in turn now can mmap head/data and
// @<Cat>/data to get bigfile view as of Cat:
//
//	mmap(bigfile/<bigfileX>/head/data)
//	mmap(bigfile/<bigfileX>/@<Cat>/data, δR(Cat,Sat), MAP_FIXED)  # mmaped at addresses corresponding to δR(Cat,Sat)
//
// When client completes its initiall mmapping it sends ack back to the server:
//
//	C: ack
//
// From now on the server will be processing updates to bigfile coming from
// ZODB as follows:
//
//
// The filesystem server itself receives information about changed data
// from ZODB server through regular ZODB invalidation channel (as it is ZODB
// client itself). Then, before actually updating bigfile/<bigfileX>/head/data
// content in changed part, it notifies through bigfile/<bigfileX>/head/invalidations
// to clients that had opened this file (separately to each client) about the changes
//
//	S: have <Sat>, δR(Sat_prev, Sat)
//
// where Sat_prev is ZODB revision last reported to client for this bigfile,
// and waits until they all confirm that changed file part can be updated in
// global OS cache.
//
// The clients in turn can now re-mmap invalidated regions to bigfile@Cat
//
//	# mmapped at addresses corresponding to δR(Sat_prev, Sat)
//	mmap(bigfile/<bigfileX>/@<Cat>/data, δR(Sat_prev, Sat), MAP_FIXED)
//
// and must send ack back to the server when they are done:
//
//	C: ack
//
// when clients are done with bigfile/<bigfileX>/@<Cat>/data (i.e. Cat
// transaction ends and array is unmapped), the server sees number of opened
// files to bigfile/<bigfileX>/@<Cat>/data and automatically destroys
// bigfile/<bigfileX>/@<Cat>/ directory	after reasonable timeout.
//
//
// Protection against slow or faulty clients
//
// If a client, on purpose or due to a bug or being stopped, is slow to
// respond with ack to invalidation notification, it creates a problem because
// head/data updates will be blocked and thus all other clients that try to
// work with current data will get stuck.
//
// To avoid this problem it should be possible for wcfs to stop a client with
// ptrace and change its address space in a style similar to e.g.
// VirtualAllocEx on windows. Here is hacky example how this could be done on Linux:
//
// https://gist.github.com/rofl0r/1073739/63f0f788a4923e26fcf743dd9a8411d4916f0ac0
//
// this way there should be no possibility for a client to block wcfs
// indefinitely waiting for client's ack.
//
// Similarly for initiall mmapings client could first mmap head/data, then open
// head/invalidations and tell the server that it wants Cat revision, with
// the server then remmaping blocks to get to Cat state via ptrace.
//
// However for simplicity the plan is to go first without ptrace and just kill
// a slow client on, say 30 seconds, timeout.
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
// 2. mmap(MAP_SHARED, PROT_READ) + write-tracking & writeout by client
//
// In this scheme bigfile data is mmaped in MAP_SHARED mode with read-only pages
// protection. Then whenever write fault occurs, client allocates RAM from
// shmfs, copies faulted page to it, and then mmaps RAM page with RW protection
// in place of original bigfile page. Writeout implementation should be similar
// to "1", only here client already knows the pages it dirtied, and this way
// there is no need to consult /proc/self/pagemap.
//
// The advantage of this scheme over mmap(MAP_PRIVATE) is that in case
// there are several mappings of the same bigfile with overlapping in-file
// ranges, changes in one mapping will be visible in another mapping.
// Contrary: whenever a MAP_PRIVATE mapping is modified, the kernel COWs
// faulted page into a page completely private to this mapping, so that other
// MAP_PRIVATE mappings of this file, including ones created from the same
// process, do not see changes made to the first mapping.
//
// Since wendelin.core needs to provide coherency in between different slices
// of the same array, this is the mode wendelin.core actually uses.
//
// 3. write to wcfs
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
