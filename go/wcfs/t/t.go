package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/fuse/nodefs"
)

// dir represents a directory in the filesystem.
type dir struct {
	nodefs.Node
}

// file represents a file in the filesystem.
type file struct {
	nodefs.Node
}

// fileHandle represents opened file.
type fileHandle struct {
	nodefs.File

	content []byte
}



func (d *dir) Lookup(out *fuse.Attr, name string, _ *fuse.Context) (*nodefs.Inode, fuse.Status) {
	ientry := d.Inode().GetChild(name)
	if ientry == nil {
		return nil, fuse.ENOENT
	}
	// XXX fill out
	return ientry, fuse.OK
}

var nopen = 0

func (f *file) Open(flags uint32, _ *fuse.Context) (nodefs.File, fuse.Status) {
	_, name := f.Inode().Parent()
	nopen++ // XXX -> atomic
	data := fmt.Sprintf("%04d %s\n", nopen, name)
	h := &fileHandle{File: nodefs.NewDefaultFile(), content: []byte(data)}
	// force direct-io to disable pagecache: we alway return different data
	// and st_size=0 (like in /proc).
	return &nodefs.WithFlags{
		File:		h,
		FuseFlags:	fuse.FOPEN_DIRECT_IO,
	}, fuse.OK
}

func (fh *fileHandle) Read(dest []byte, off int64) (fuse.ReadResult, fuse.Status) {
	l := int64(len(dest))

	// XXX demonstrate we can indeed serve different content to different openings.
	if l >= 1 {
		l = 1
		time.Sleep(1*time.Second)
	}

	end := off + l
	if ldata := int64(len(fh.content)); end > ldata {
		end = ldata
	}

	res := fh.content[off:end]
	fmt.Printf("read [%d:%d] -> %q\n", off, end, res)
	return fuse.ReadResultData(res), fuse.OK
}


func (d *dir) mkdir(name string) *dir {
	child := &dir{Node: nodefs.NewDefaultNode()}
	d.Inode().NewChild(name, true, child)
	return child
}

func (d *dir) mkfile(name string) *file {
	child := &file{Node: nodefs.NewDefaultNode()}
	d.Inode().NewChild(name, false, child)
	return child
}


func main() {
	debug := flag.Bool("d", true, "debug")
	flag.Parse()
	if len(flag.Args()) != 1 {
		log.Fatalf("Usage: %s mntpt", os.Args[0])
	}
	mntpt := flag.Args()[0]
	root  := &dir{Node: nodefs.NewDefaultNode()}
	opts := nodefs.NewOptions()
	if *debug {
		opts.Debug = true
	}
	server, _, err := nodefs.MountRoot(mntpt, root, opts)
	if err != nil {
		log.Fatal(err)	// XXX err ctx?
	}

	// NOTE cannot make entries before mount because Inode.AddChild does
	// not work before that (panics on nil deref to mountRootXXX)
	root.mkdir("aaa")
	root.mkfile("hello.txt")

	server.Serve()	// XXX error?
}
