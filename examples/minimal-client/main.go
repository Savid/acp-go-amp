package main

import (
	"context"
	"os"

	ampacp "github.com/savid/acp-go-amp"
)

func main() {
	_ = ampacp.Serve(context.Background(), os.Stdin, os.Stdout)
}
