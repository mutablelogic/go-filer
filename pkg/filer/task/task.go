package task

import (
	media "github.com/mutablelogic/go-media/pkg/ffmpeg"
)

///////////////////////////////////////////////////////////////////////////////
// TYPES

const (
	TaskNamespace          = "filer"
	TaskNameRegisterObject = "registerobject"
)

type taskrunner struct {
	manager *media.Manager
}

///////////////////////////////////////////////////////////////////////////////
// LIFECYCLE

func NewTaskRunner() (*taskrunner, error) {
	self := new(taskrunner)

	// Create a media manager
	manager, err := media.NewManager()
	if err != nil {
		return nil, err
	} else {
		self.manager = manager
	}

	return self, nil
}
