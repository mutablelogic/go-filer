package task

import (
	"context"
	"time"

	// Packages
	filer "github.com/mutablelogic/go-filer"
	schema "github.com/mutablelogic/go-filer/pkg/filer/schema"
	ffmpeg "github.com/mutablelogic/go-media/pkg/ffmpeg"
	server "github.com/mutablelogic/go-server"
	queue_schema "github.com/mutablelogic/go-server/pkg/pgqueue/schema"
	ref "github.com/mutablelogic/go-server/pkg/ref"
	types "github.com/mutablelogic/go-server/pkg/types"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

const (
	TaskNamespace            = "filer"
	TaskNameRegisterObject   = "registerobject"
	TaskNameAnalyseMedia     = "analyzemedia"
	TaskNameAnalyseImageEXIF = "analyzeimage_exif"
	TaskNameAnalysePDF       = "analyzepdf"
	TaskNameAnalyseText      = "analyzetext"
	TaskNameAnalyseHTML      = "analyzehtml"
)

type taskrunner struct {
	queue server.PGQueue
	filer filer.Filer
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func NewTaskRunner(ctx context.Context, filer filer.Filer, queue server.PGQueue) (*taskrunner, error) {
	self := new(taskrunner)
	self.filer = filer
	self.queue = queue
	ffmpeg.SetLogging(false, func(text string) {
		ref.Log(ctx).Printf(ctx, "ffmpeg: %s", text)
	})

	// Register tasks
	taskMap := map[string]func(context.Context, *schema.Object) error{
		TaskNameRegisterObject:   self.RegisterObject,
		TaskNameAnalyseMedia:     self.AnalyseMedia,
		TaskNameAnalyseImageEXIF: self.AnalyseImageEXIF,
		TaskNameAnalysePDF:       self.AnalysePDF,
		TaskNameAnalyseText:      self.AnalyseText,
		TaskNameAnalyseHTML:      self.AnalyseHTML,
	}

	for task, fn := range taskMap {
		if _, err := self.queue.RegisterQueue(ctx, queue_schema.QueueMeta{
			Queue:      task,
			TTL:        types.DurationPtr(time.Hour),
			Retries:    types.Uint64Ptr(3),
			RetryDelay: types.DurationPtr(time.Minute),
		}, func(ctx context.Context, in any) error {
			var object schema.Object
			if err := self.queue.UnmarshalPayload(&object, in); err != nil {
				return err
			}
			return fn(ctx, &object)
		}); err != nil {
			return nil, err
		}
	}

	// Return success
	return self, nil
}

///////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

func (t *taskrunner) queueTask(ctx context.Context, task string, object *schema.Object) error {
	if _, err := t.queue.CreateTask(ctx, task, object, 0); err != nil {
		return err
	}
	return nil
}
