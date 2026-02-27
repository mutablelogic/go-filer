package version

import (
	"encoding/json"
	"runtime"
	"runtime/debug"
)

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

var (
	GitSource   string
	GitTag      string
	GitBranch   string
	GitHash     string
	GoBuildTime string
)

///////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Version returns the best available version string. It prefers the git tag
// set via -ldflags, then the branch, then the short VCS revision from the
// embedded build info, and finally falls back to "dev".
func Version() string {
	if GitTag != "" {
		return GitTag
	}
	if GitBranch != "" {
		return GitBranch
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			if s.Key == "vcs.revision" && s.Value != "" {
				if len(s.Value) > 12 {
					return s.Value[:12]
				}
				return s.Value
			}
		}
	}
	return "dev"
}

// JSON returns a JSON-encoded metadata blob for the given executable name.
func JSON(execName string) []byte {
	metadata := map[string]string{
		"name":     execName,
		"version":  Version(),
		"compiler": runtime.Version(),
	}

	if GitSource != "" {
		metadata["source"] = GitSource
	}
	if GitTag != "" {
		metadata["tag"] = GitTag
	}
	if GitBranch != "" {
		metadata["branch"] = GitBranch
	}
	if GitHash != "" {
		metadata["hash"] = GitHash
	}
	if GoBuildTime != "" {
		metadata["build_time"] = GoBuildTime
	}

	// Fill in any missing fields from embedded build info
	if info, ok := debug.ReadBuildInfo(); ok {
		if _, ok := metadata["source"]; !ok && info.Main.Path != "" {
			metadata["source"] = info.Main.Path
		}
		var goos, goarch string
		for _, s := range info.Settings {
			switch s.Key {
			case "vcs.revision":
				if _, ok := metadata["hash"]; !ok && s.Value != "" {
					metadata["hash"] = s.Value
				}
			case "vcs.time":
				if _, ok := metadata["build_time"]; !ok && s.Value != "" {
					metadata["build_time"] = s.Value
				}
			case "vcs.modified":
				if s.Value == "true" {
					metadata["modified"] = s.Value
				}
			case "GOOS":
				goos = s.Value
			case "GOARCH":
				goarch = s.Value
			}
		}
		if goos != "" && goarch != "" {
			metadata["platform"] = goos + "/" + goarch
		}
	}

	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		panic(err)
	}
	return data
}
