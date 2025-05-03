package task

import (
	"context"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	ffmpeg "github.com/mutablelogic/go-media/pkg/ffmpeg"
	"github.com/mutablelogic/go-server/pkg/ref"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

const (
	TaskNamespace          = "filer"
	TaskNameRegisterObject = "registerobject"
)

type taskrunner struct {
	filer filer.Filer
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func NewTaskRunner(ctx context.Context, filer filer.Filer) (*taskrunner, error) {
	self := new(taskrunner)
	self.filer = filer
	ffmpeg.SetLogging(false, func(text string) {
		ref.Log(ctx).Printf(ctx, "ffmpeg: %s", text)
	})
	return self, nil
}
