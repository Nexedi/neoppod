- custom memory manager is required to avoid performance hit on e.g. []byte alloc/dealloc

  -> go-slab

- interfaces conversion in hot codepaths are costly:

  e.g. having r as io.ReadSeeker and converting it to io.Reader e.g. this way:


	io.ReadFull(r, ...)

  calls convI2I all the time (no caching) which is not negliable



- python pickles

	stalecucmber
	og-rek
	graphite-clickhous/helper/pickle.go

	particular fsIndex: by hand

- file size / file offset (off_t) is defined to be _signed_ by posix

  -> so use int64 (not uint64) for file pos / size

- "Go Execution Tracer"
