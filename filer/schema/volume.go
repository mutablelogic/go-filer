package schema

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"

	// Packages
	tui "github.com/mutablelogic/go-server/pkg/tui"
	types "github.com/mutablelogic/go-server/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////
// GLOBALS

// The statuses of an index
const (
	IndexStatusNone     IndexStatus = ""         // It's never been indexed
	IndexStatusScanning IndexStatus = "scanning" // It's being scanned, for the first time, or again
	IndexStatusReady    IndexStatus = "ready"    // It's indexed, and the index follows its changes
)

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

// VolumeMeta is what's kept of a volume, so it outlasts a restart: its name,
// the URL of its backend, which it's added again from, and whether it's
// mounted when the server starts
type VolumeMeta struct {
	Name    string `json:"name" help:"Volume name"`
	URL     string `json:"url" help:"URL of the volume's backend, with each of its options, defaults too, such as index, but without secrets"`
	Enabled bool   `json:"enabled,omitempty" help:"Whether the volume is mounted, when the server starts"`
}

// Volume is a volume which is registered, or stored
type Volume struct {
	VolumeMeta
	Created  time.Time    `json:"created,omitzero" readonly:"" help:"When the volume was first stored"`
	Modified time.Time    `json:"modified,omitzero" readonly:"" help:"When the volume's URL, or whether it's enabled, last changed"`
	Error    string       `json:"error,omitempty" readonly:"" help:"Why the volume isn't enabled, such as when watching its backend failed, until it's opened again, or its user needs to sign in again, which volume-update does"`
	Index    *VolumeIndex `json:"index,omitempty" readonly:"" help:"The volume's index, when it's indexed, of its index option, such as index=folders"`
}

// Readonly returns true when the volume's objects can't be created or
// deleted, of its URL's readonly option, which is true when it's empty, or
// there's none. Returns an error when the URL, or the option, can't be
// parsed.
func (v Volume) Readonly() (bool, error) {
	u, err := url.Parse(v.URL)
	if err != nil {
		return true, fmt.Errorf("volume %q: invalid url", v.Name)
	}
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return true, fmt.Errorf("volume %q: invalid options", v.Name)
	}
	value := query.Get("readonly")
	if value == "" {
		return true, nil
	}
	readonly, err := strconv.ParseBool(value)
	if err != nil {
		return true, fmt.Errorf("volume %q: invalid readonly option %q", v.Name, value)
	}
	return readonly, nil
}

// VolumeIndex is a volume's index: what's indexed, and its state
type VolumeIndex struct {
	Mode string `json:"mode" help:"What's indexed, such as folders"`
	IndexState
}

// IndexState is the state of a volume's index
type IndexState struct {
	Status  IndexStatus `json:"status,omitempty" help:"How far it's indexed: scanning, or ready, or empty, when it hasn't been yet"`
	Folders uint64      `json:"folders,omitempty" help:"How many folders the scan in progress, or the last, has indexed"`
	Started time.Time   `json:"started,omitzero" help:"When the scan in progress, or the last, started"`
	Scanned time.Time   `json:"scanned,omitzero" help:"When the last scan which finished did"`
	Error   string      `json:"error,omitempty" help:"Why the last scan, or the watch, failed"`
}

// IndexStatus is how far a volume is indexed
type IndexStatus string

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
