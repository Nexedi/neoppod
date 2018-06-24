package main

import (
	"flag"
	"log"
	"os"

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
}



func (d *dir) Lookup(out *fuse.Attr, name string, ctx *fuse.Context) (*nodefs.Inode, fuse.Status) {
	ientry := d.Inode().GetChild(name)
	if ientry == nil {
		return nil, fuse.ENOENT
	}
	// XXX fill out
	return ientry, fuse.OK
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
	//nodefs.NewInode(true, root)
	//println(root.Inode())

	opts := nodefs.NewOptions()
	if *debug {
		opts.Debug = true
	}
	server, _, err := nodefs.MountRoot(mntpt, root, opts)
	if err != nil {
		log.Fatal(err)	// XXX err ctx?
	}

	root.mkdir("aaa")
	root.mkfile("hello.txt")

	server.Serve()	// XXX error?
}
