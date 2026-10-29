package web

import (
	"embed"
	"io/fs"
)

//go:embed templates/*.html static/*
var Files embed.FS

func Static() fs.FS {
	sub, err := fs.Sub(Files, "static")
	if err != nil {
		panic(err)
	}
	return sub
}
