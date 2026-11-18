package schema

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"

	// Packages
	tui "github.com/mutablelogic/go-server/pkg/tui"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

// The schemes of the URLs of volumes' backends
const (
	// SchemeFile is the scheme of a volume which is a directory, such as
	// file://media/mnt/media
	SchemeFile = "file"

	// SchemeFS is the scheme of a volume which is a filesystem, but not a
	// directory, such as fs://media, which a volume can't be added from
	SchemeFS = "fs"

	// SchemeS3 is the scheme of a volume which is a bucket, and the prefix of
	// its objects, such as s3://my-bucket/photos?name=photos
	SchemeS3 = "s3"
)

////////////////////////////////////////////////////////////////////////////////
// TYPES

// Volume is a volume which is registered, with the URL of its backend, which
// it can be added again from
type Volume struct {
	Name     string `json:"name" help:"Volume name"`
	URL      string `json:"url" help:"URL of the volume's backend, with the options which aren't the defaults"`
	Readonly bool   `json:"readonly,omitempty" readonly:"" help:"Objects can't be created or deleted, by its options, or as its backend can't be written"`
}

// VolumeList is the volumes which are registered, in order of their names,
// and the schemes of the URLs which volumes can be added from
type VolumeList struct {
	Body    []Volume `json:"body" help:"Volumes, in order of their names"`
	Schemes []string `json:"schemes" help:"Schemes of the URLs which volumes can be added from, such as file"`
}

////////////////////////////////////////////////////////////////////////////////
// STRINGIFY

func (v Volume) String() string {
	return types.Stringify(v)
}

func (v VolumeList) String() string {
	return types.Stringify(v)
}

////////////////////////////////////////////////////////////////////////////////
// TABLE

func (v Volume) Header() []string {
	return []string{"Name", "URL"}
}

func (v Volume) Width(col int) int {
	return 0
}

func (v Volume) Cell(col int) tui.CellText {
	switch col {
	case 0:
		return v.Name
	case 1:
		return v.URL
	default:
		return ""
	}
}

// VolumePatch is the options of a volume to change, by their keys, as JSON:
// null removes the option, so it's the default, a list replaces its values,
// where an empty list has none, and a string, number or boolean replaces
// its values with the one
type VolumePatch map[string]json.RawMessage

////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS

// Values returns the options to change, where an option without values is
// removed, or an error when one isn't null, a list of strings,
// numbers and booleans, or a string, number or boolean. An empty list is an
// empty value, so for skip and package, there are none.
func (p VolumePatch) Values() (url.Values, error) {
	values := make(url.Values, len(p))
	for key, raw := range p {
		if key == "" {
			return nil, errors.New("option without a key")
		}
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, fmt.Errorf("option %q: %w", key, err)
		}
		switch value := value.(type) {
		case nil:
			values[key] = []string{}
		case []any:
			if len(value) == 0 {
				values[key] = []string{""}
				continue
			}
			for _, elem := range value {
				str, ok := scalar(elem)
				if !ok {
					return nil, fmt.Errorf("option %q has a value which isn't a string, number or boolean", key)
				}
				values[key] = append(values[key], str)
			}
		default:
			str, ok := scalar(value)
			if !ok {
				return nil, fmt.Errorf("option %q isn't null, a list, or a string, number or boolean", key)
			}
			values[key] = []string{str}
		}
	}
	return values, nil
}

////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS

// scalar returns a string, number or boolean of JSON as a string
func scalar(value any) (string, bool) {
	switch value := value.(type) {
	case string:
		return value, true
	case bool:
		return strconv.FormatBool(value), true
	case float64:
		return strconv.FormatFloat(value, 'f', -1, 64), true
	default:
		return "", false
	}
}
