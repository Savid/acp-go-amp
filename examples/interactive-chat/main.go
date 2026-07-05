package main

import (
	"context"
	"io"
	"os"

	ampacp "github.com/savid/acp-go-amp"
)

var serve = func(ctx context.Context, input io.Reader, output io.Writer) error {
	return ampacp.Serve(ctx, input, output)
}

func main() {
	_ = serve(context.Background(), os.Stdin, os.Stdout)
}
