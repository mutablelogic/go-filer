package task

import (
	filer "github.com/mutablelogic/go-filer"
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

func NewTaskRunner(filer filer.Filer) (*taskrunner, error) {
	self := new(taskrunner)
	self.filer = filer
	return self, nil
}
