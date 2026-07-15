package ampacp

import nativeamp "github.com/savid/acp-go-amp/internal/amp"

// ErrProcessTreeUnproven reports that the adapter could not prove every
// native descendant exited. Callers must retain resources that may still be
// reachable by the process tree.
var ErrProcessTreeUnproven = nativeamp.ErrProcessTreeNotQuiescent
