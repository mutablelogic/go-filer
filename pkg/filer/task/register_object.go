package task

import (
	"context"
	"fmt"
)

func RegisterObject(ctx context.Context, in any) error {
	fmt.Println("RegisterObject called", in)
	return nil
}
