package main

import (
	"context"
	"os"

	ampacp "github.com/savid/acp-go-amp"
)

var serve = ampacp.Serve

func main() {
	_ = serve(context.Background(), os.Stdin, os.Stdout, ampacp.WithSessionStore(ampacp.NewInMemorySessionStore()))
}
