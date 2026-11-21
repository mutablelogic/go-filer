package schema

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strconv"
	"strings"
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

// The phases of a scan of an index
const (
	IndexPhaseIdle     IndexPhase = ""         // It isn't scanned
	IndexPhaseScanning IndexPhase = "scanning" // Its folders, and files, are listed
	IndexPhaseIndexing IndexPhase = "indexing" // Its files which are new, or changed, are got, and stored
)

// OptDescription is the option of every backend's URL which describes what
// the volume is, such as "Folder /mnt/media", which a backend sets, when
// it isn't set
const OptDescription = "description"

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
	Enabled bool   `json:"enabled" help:"Whether the volume is mounted, when the server starts"`
}

// VolumeCreateRequest is a volume to create, which is enabled, unless it
// says it isn't
type VolumeCreateRequest struct {
	Name    string `json:"name" help:"Volume name"`
	URL     string `json:"url" help:"URL of the volume's backend, with its options"`
	Enabled *bool  `json:"enabled,omitempty" help:"Whether the volume is mounted, which it is by default"`
}

// Meta returns what's stored of the volume to create
func (r VolumeCreateRequest) Meta() VolumeMeta {
	return VolumeMeta{Name: r.Name, URL: r.URL, Enabled: r.Enabled == nil || *r.Enabled}
}

// Volume is a volume which is registered, or stored
type Volume struct {
	VolumeMeta
	Created  time.Time    `json:"created,omitzero" readonly:"" help:"When the volume was first stored"`
	Modified time.Time    `json:"modified,omitzero" readonly:"" help:"When the volume's URL, or whether it's enabled, last changed"`
	Error    string       `json:"error,omitempty" readonly:"" help:"Why the volume isn't enabled, such as when watching its backend failed, until it's opened again, or its user needs to sign in again, which volume-update does"`
	Mounted  bool         `json:"mounted" readonly:"" help:"Whether the volume is mounted, so its objects can be used, which an enabled volume is shortly after it's created, or updated"`
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

// Description returns what the volume is, of its URL's description option,
// or empty, when it hasn't one, or its URL can't be parsed
func (v Volume) Description() string {
	u, err := url.Parse(v.URL)
	if err != nil {
		return ""
	}
	return u.Query().Get(OptDescription)
}

// VolumeIndex is a volume's index: what's indexed, and its state
type VolumeIndex struct {
	Mode string `json:"mode" help:"What's indexed, such as folders"`
	IndexState
	Metadata *TaskState `json:"metadata,omitempty" help:"The metadata of the files, which is indexed of each file, apart from scans, of index=metadata"`
}

// TaskState is the work of a task, such as indexing the metadata of files,
// of a volume's files
type TaskState struct {
	Queued  uint64 `json:"queued,omitempty" help:"How many files are queued, or being done, including those which failed, and are tried again"`
	Indexed uint64 `json:"indexed,omitempty" help:"How many files it's done of, of their revisions, including those it failed of too often, which are given up until they change"`
	Failed  uint64 `json:"failed,omitempty" help:"How many files it failed of too often, which are given up until they change"`
	Error   string `json:"error,omitempty" help:"Why it last failed of a file"`
}

// IndexState is the state of a volume's index
type IndexState struct {
	Status  IndexStatus `json:"status,omitempty" help:"How far it's indexed: scanning, or ready, or empty, when it hasn't been yet"`
	Folders uint64      `json:"folders,omitempty" help:"How many folders the scan in progress, or the last, has indexed"`
	Files   uint64      `json:"files,omitempty" help:"How many files the last scan indexed, of a volume whose files are indexed"`
	Mode    string      `json:"mode,omitempty" help:"What the last scan which finished indexed: folders, or objects"`
	Phase   IndexPhase  `json:"phase,omitempty" help:"What the scan in progress is doing: scanning, when it's listed, or indexing, when its files are stored, or empty, when it's idle"`
	Started time.Time   `json:"started,omitzero" help:"When the scan in progress, or the last, started"`
	Scanned time.Time   `json:"scanned,omitzero" help:"When the last scan which finished did"`
	Error   string      `json:"error,omitempty" help:"Why the last scan, or the watch, failed"`
}

// IndexStatus is how far a volume is indexed
type IndexStatus string

// IndexPhase is what a scan of a volume's index is doing
type IndexPhase string

// VolumeList is the volumes which are registered, in order of their names,
// and the backends which volumes can be added of
type VolumeList struct {
	Body     []Volume  `json:"body" help:"Volumes, in order of their names"`
	Backends []Backend `json:"backends" help:"Backends which volumes can be added of, in order of their schemes"`
}

// Backend is a type of backend, of the scheme of its volumes' URLs, such as
// file, and what it is
type Backend struct {
	Scheme      string `json:"scheme" help:"Scheme of the URLs of its volumes, such as file"`
	Description string `json:"description,omitempty" help:"What it is, such as Server filesystem"`
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

func (b Backend) Header() []string {
	return []string{"Scheme", "Description"}
}

func (b Backend) Width(col int) int {
	return 0
}

func (b Backend) Cell(col int) tui.CellText {
	switch col {
	case 0:
		return b.Scheme
	case 1:
		return b.Description
	default:
		return ""
	}
}

func (v Volume) Header() []string {
	return []string{"Name", "Status", "Index", "Description", "URL", "Options"}
}

func (v Volume) Width(col int) int {
	return 0
}

func (v Volume) Cell(col int) tui.CellText {
	switch col {
	case 0:
		return v.Name
	case 1:
		return v.Status()
	case 2:
		return v.IndexStatus()
	case 3:
		return v.Description()
	case 4:
		return v.BareURL()
	case 5:
		return v.Options()
	default:
		return ""
	}
}

// Status returns how the volume is: offline, with why, when it's enabled,
// but its backend isn't working, disabled, mounting, when it's enabled, but
// isn't mounted yet, or mounted
func (v Volume) Status() string {
	switch {
	case v.Error != "":
		return "offline: " + v.Error
	case !v.Enabled:
		return "disabled"
	case !v.Mounted:
		return "mounting"
	default:
		return "mounted"
	}
}

// IndexStatus returns how the volume's index is: none, when it isn't
// indexed, or isn't mounted, indexing, when it's scanned, for the first
// time, with its progress, or again, with the counts of its last scan, or
// idle, when it's ready, with how many folders it has, or failed, when it hasn't been indexed, with why its last scan, or
// its watch, failed, when it did
func (v Volume) IndexStatus() string {
	var result string
	switch {
	case v.Index == nil:
		return "none"
	case v.Index.Status == IndexStatusScanning || v.Index.Status == IndexStatusReady:
		// What its scan is doing, of the counts of the scan in progress, the
		// first, or of its last scan, when it's ready, which is idle, when
		// it isn't scanned
		// it isn't scanned, unless the metadata of its files is queued, which
		// is indexing
		phase := string(v.Index.Phase)
		if phase == "" && v.Index.Metadata != nil && v.Index.Metadata.Queued > 0 {
			phase = string(IndexPhaseIndexing)
		} else if phase == "" {
			phase = "idle"
		}
		result = fmt.Sprintf("%s, %d folders", phase, v.Index.Folders)
		if (v.Index.Mode == "objects" || v.Index.Mode == "metadata") && (v.Index.Status == IndexStatusReady || v.Index.Files > 0) {
			result += fmt.Sprintf(", %d objects", v.Index.Files)
		}
		if task := v.Index.Metadata; task != nil {
			result += fmt.Sprintf(", %d indexed", task.Indexed)
			if task.Queued > 0 {
				result += fmt.Sprintf(", %d queued", task.Queued)
			}
			if task.Failed > 0 {
				result += fmt.Sprintf(", %d failed", task.Failed)
			}
		}
	case v.Index.Error != "":
		result = "failed"
	default:
		result = "none"
	}
	if v.Index.Error != "" {
		result += ": " + v.Index.Error
	}
	return result
}

// BareURL returns the volume's URL without its options, such as
// file://media/mnt/media, or the URL, when it can't be parsed
func (v Volume) BareURL() string {
	u, err := url.Parse(v.URL)
	if err != nil {
		return v.URL
	}
	u.RawQuery, u.Fragment = "", ""
	return u.String()
}

// Options returns the options of the volume's URL, other than its
// description, as key=value, in the order of their keys, separated by
// spaces, without escaping
func (v Volume) Options() string {
	u, err := url.Parse(v.URL)
	if err != nil {
		return ""
	}
	query := u.Query()
	query.Del(OptDescription)
	keys := slices.Sorted(maps.Keys(query))
	result := make([]string, 0, len(query))
	for _, key := range keys {
		for _, value := range query[key] {
			result = append(result, key+"="+value)
		}
	}
	return strings.Join(result, " ")
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
