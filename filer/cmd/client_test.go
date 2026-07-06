package cmd

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestObjectUploadPath(t *testing.T) {
	tests := []struct {
		name string
		path string
		want string
	}{
		{
			name: "absolute path without anchor",
			path: "/Volumes/Scratch/Outbox/Ujetz Family Movie.mp4",
			want: "Volumes/Scratch/Outbox/Ujetz Family Movie.mp4",
		},
		{
			name: "absolute path with anchor",
			path: "/Volumes/Scratch/./Outbox/Ujetz Family Movie.mp4",
			want: "Outbox/Ujetz Family Movie.mp4",
		},
		{
			name: "relative path with leading anchor",
			path: "./Outbox/Ujetz Family Movie.mp4",
			want: "Outbox/Ujetz Family Movie.mp4",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, objectUploadPath(tt.path))
		})
	}
}
