package cmd

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	// Packages
	kong "github.com/alecthomas/kong"
	gofiler "github.com/mutablelogic/go-filer"
	httpclient "github.com/mutablelogic/go-filer/registry/httpclient"
	schema "github.com/mutablelogic/go-filer/schema"
	assert "github.com/stretchr/testify/assert"
	require "github.com/stretchr/testify/require"
)

func Test_VolumeCreateCmd(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)
	// abs returns the URL path of a directory, which is absolute, which on
	// Windows has a drive, so /mnt/media is /C:/mnt/media
	abs := func(path string) string {
		path, err := filepath.Abs(path)
		require.NoError(t, err)
		if path = filepath.ToSlash(path); path[0] != '/' {
			path = "/" + path
		}
		return path
	}

	// volume parses the arguments, and returns the volume to create
	volume := func(args ...string) (string, string, error) {
		var cli struct {
			VolumeCreate VolumeCreateCmd `cmd:"" name:"volume-create"`
		}
		parser, err := kong.New(&cli)
		require.NoError(t, err)
		if _, err := parser.Parse(append([]string{"volume-create"}, args...)); err != nil {
			return "", "", err
		}
		volume, err := cli.VolumeCreate.volume()
		return volume.Name, volume.URL, err
	}

	// The name of a file URL is its host, or --name, which is the host of a
	// URL without one, and a directory is a file URL, which is absolute.
	// Otherwise the name is the name option, or the host, or --name, which
	// sets the option when it isn't the host.
	for _, test := range []struct {
		args       []string
		name, want string
	}{
		{[]string{"file://media/mnt/media"}, "media", "file://media/mnt/media"},
		{[]string{"file://media/mnt/media", "--name", "media"}, "media", "file://media/mnt/media"},
		{[]string{"file:///mnt/media", "--name", "media"}, "media", "file://media/mnt/media"},
		{[]string{"/mnt/media", "--name", "media"}, "media", "file://media" + abs(filepath.FromSlash("/mnt/media"))},
		{[]string{"/mnt/media?readonly=true", "--name", "media"}, "media", "file://media" + abs(filepath.FromSlash("/mnt/media")) + "?readonly=true"},
		{[]string{"music", "--name", "media"}, "media", "file://media" + abs(filepath.Join(wd, "music"))},
		{[]string{"s3://bucket/photos?name=media"}, "media", "s3://bucket/photos?name=media"},
		{[]string{"s3://bucket/photos"}, "bucket", "s3://bucket/photos"},
		{[]string{"s3://bucket/photos", "--name", "bucket"}, "bucket", "s3://bucket/photos"},
		{[]string{"s3://bucket/photos", "--name", "media"}, "media", "s3://bucket/photos?name=media"},
		{[]string{"s3://bucket/photos?name=media", "--name", "media"}, "media", "s3://bucket/photos?name=media"},
		{[]string{"s3://bucket/photos", "-o", "name=media", "-o", "region=eu-west-1"}, "media", "s3://bucket/photos?name=media&region=eu-west-1"},
	} {
		name, url, err := volume(test.args...)
		require.NoError(t, err, test.args)
		assert.Equal(t, test.name, name, test.args)
		assert.Equal(t, test.want, url, test.args)
	}

	// Options are added to those of the URL, and can be repeated, and an
	// option without a value is empty
	for _, test := range []struct {
		args []string
		want string
	}{
		{[]string{"file://media/mnt/media", "--opt", "readonly=true"}, "readonly=true"},
		{[]string{"file://media/mnt/media?readonly=true", "-o", "package="}, "package=&readonly=true"},
		{[]string{"file://media/mnt/media", "--opt", "readonly"}, "readonly="},
		{[]string{"file://media/mnt/media", "--opt", `skip=^\.`, "--opt", `skip=\.tmp$`}, "skip=%5E%5C.&skip=%5C.tmp%24"},
	} {
		_, url, err := volume(test.args...)
		require.NoError(t, err, test.args)
		assert.Equal(t, "file://media/mnt/media?"+test.want, url, test.args)
	}

	// A volume without a name, or whose name isn't the URL's host, or its
	// name option, or with
	// an option without a key, isn't created, and the URL is needed
	for _, args := range [][]string{
		{"file:///mnt/media"},
		{"/mnt/media"},
		{"file://media/mnt/media", "--name", "other"},
		{"s3:///photos"},
		{"s3://bucket/photos?name=media", "--name", "other"},
		{"file://media/mnt/media", "--opt", "=true"},
		{"file://media/mnt/media?readonly=%ZZ", "--opt", "package="},
		{},
	} {
		_, _, err := volume(args...)
		assert.Error(t, err, args)
	}
}

func Test_VolumeEnabled(t *testing.T) {
	var cli struct {
		VolumeUpdate  VolumeUpdateCmd  `cmd:"" name:"volume-update"`
		VolumeMount   VolumeMountCmd   `cmd:"" name:"volume-mount"`
		VolumeUnmount VolumeUnmountCmd `cmd:"" name:"volume-unmount"`
	}
	parse := func(args ...string) {
		t.Helper()
		cli.VolumeUpdate.Enabled = nil
		parser, err := kong.New(&cli)
		require.NoError(t, err)
		_, err = parser.Parse(args)
		require.NoError(t, err)
	}

	// Whether a volume is enabled is kept, unless it's updated with
	// --enabled, or --no-enabled
	parse("volume-update", "media")
	assert.Nil(t, cli.VolumeUpdate.Enabled)
	parse("volume-update", "media", "--enabled")
	if assert.NotNil(t, cli.VolumeUpdate.Enabled) {
		assert.True(t, *cli.VolumeUpdate.Enabled)
	}
	parse("volume-update", "media", "--no-enabled")
	if assert.NotNil(t, cli.VolumeUpdate.Enabled) {
		assert.False(t, *cli.VolumeUpdate.Enabled)
	}

	// A volume is mounted, and unmounted, by its name
	parse("volume-mount", "music")
	assert.Equal(t, "music", cli.VolumeMount.Name)
	parse("volume-unmount", "photos")
	assert.Equal(t, "photos", cli.VolumeUnmount.Name)
}

func Test_VolumeUpdateCmd(t *testing.T) {
	patch := func(args ...string) (map[string]string, error) {
		var cli struct {
			VolumeUpdate VolumeUpdateCmd `cmd:"" name:"volume-update"`
		}
		parser, err := kong.New(&cli)
		require.NoError(t, err)
		if _, err := parser.Parse(append([]string{"volume-update", "media"}, args...)); err != nil {
			return nil, err
		}
		patch, err := cli.VolumeUpdate.patch()
		if err != nil {
			return nil, err
		}
		result := map[string]string{}
		for key, value := range patch {
			result[key] = string(value)
		}
		return result, nil
	}

	// Each option has the values it's given, where one without a value has
	// none, and an option to remove is null
	got, err := patch("-o", "readonly=true", "-o", `skip=^\.`, "-o", `skip=\.tmp$`, "-o", "package", "--unset", "other")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"readonly": `["true"]`,
		"skip":     `["^\\.","\\.tmp$"]`,
		"package":  `[]`,
		"other":    `null`,
	}, got)

	// No options is an empty patch, which opens the volume again
	got, err = patch()
	require.NoError(t, err)
	assert.Empty(t, got)

	// An option without a key, or one which is set and removed, isn't a
	// patch
	for _, args := range [][]string{{"-o", "=true"}, {"--unset", ""}, {"-o", "readonly=true", "--unset", "readonly"}} {
		_, err := patch(args...)
		assert.Error(t, err, args)
	}
}

func Test_VolumeOptComma(t *testing.T) {
	var cli struct {
		VolumeCreate VolumeCreateCmd `cmd:"" name:"volume-create"`
	}
	parser, err := kong.New(&cli)
	require.NoError(t, err)

	// An option with a comma, such as a regular expression, isn't split
	_, err = parser.Parse([]string{"volume-create", "file://media/mnt/media", "-o", `skip=^(a,b)$`})
	require.NoError(t, err)
	assert.Equal(t, []string{`skip=^(a,b)$`}, cli.VolumeCreate.Opt)
}

func Test_VolumeCreateFSCmd(t *testing.T) {
	var cli struct {
		VolumeCreateFS VolumeCreateFSCmd `cmd:"" name:"volume-create-fs"`
	}
	parser, err := kong.New(&cli)
	require.NoError(t, err)
	volume := func(args ...string) (string, error) {
		t.Helper()
		cli.VolumeCreateFS = VolumeCreateFSCmd{}
		_, err := parser.Parse(append([]string{"volume-create-fs"}, args...))
		require.NoError(t, err)
		v, err := cli.VolumeCreateFS.volume()
		if err != nil {
			return "", err
		}
		assert.Equal(t, args[0], v.Name)
		return v.URL, nil
	}

	// A directory on the server, with its options, which are repeated, and
	// a regular expression, with a comma, which isn't split, or empty
	u, err := volume("media", "/mnt/media")
	require.NoError(t, err)
	assert.Equal(t, "file://media/mnt/media", u)
	u, err = volume("media", "/mnt/media", "--readonly", "--skip", `^(a,b)$`, "--skip", `^\.`, "--package", "")
	require.NoError(t, err)
	assert.Equal(t, "file://media/mnt/media?package=&readonly=true&skip=%5E%28a%2Cb%29%24&skip=%5E%5C.", u)

	// Another option, which no flag sets, and one which replaces a flag's
	u, err = volume("media", "/mnt/media", "--readonly", "-o", "index=folders", "-o", "readonly=false")
	require.NoError(t, err)
	assert.Equal(t, "file://media/mnt/media?index=folders&readonly=false", u)

	// A directory on Windows, with a drive, and one on Unix, whose name has
	// a backslash, and a space, which are kept
	u, err = volume("media", `C:\media`)
	require.NoError(t, err)
	assert.Equal(t, "file://media/C:/media", u)
	u, err = volume("media", `/mnt/a\b `)
	require.NoError(t, err)
	assert.Equal(t, "file://media/mnt/a%5Cb%20", u)

	// A relative path, which can't be the server's, and a name which isn't
	// an identifier
	_, err = volume("media", "media")
	assert.Error(t, err)
	_, err = volume("not-a-name!", "/mnt/media")
	assert.Error(t, err)
}

func Test_VolumeCreateS3Cmd(t *testing.T) {
	var cli struct {
		VolumeCreateS3 VolumeCreateS3Cmd `cmd:"" name:"volume-create-s3"`
	}
	parser, err := kong.New(&cli)
	require.NoError(t, err)
	volume := func(args ...string) (string, error) {
		t.Helper()
		cli.VolumeCreateS3 = VolumeCreateS3Cmd{}
		_, err := parser.Parse(append([]string{"volume-create-s3"}, args...))
		require.NoError(t, err)
		v, err := cli.VolumeCreateS3.volume()
		if err != nil {
			return "", err
		}
		assert.Equal(t, args[0], v.Name)
		return v.URL, nil
	}

	// A bucket, whose name is the volume's, and one which isn't, with a
	// prefix, which is the name option
	u, err := volume("photos", "photos")
	require.NoError(t, err)
	assert.Equal(t, "s3://photos", u)
	u, err = volume("photos", "photos", "-o", "index=folders")
	require.NoError(t, err)
	assert.Equal(t, "s3://photos?index=folders", u)
	u, err = volume("photos", "my.bucket/photos/2026/")
	require.NoError(t, err)
	assert.Equal(t, "s3://my.bucket/photos/2026?name=photos", u)

	// A prefix which ends with a space, which is kept
	u, err = volume("photos", "my.bucket/photos ")
	require.NoError(t, err)
	assert.Equal(t, "s3://my.bucket/photos%20?name=photos", u)

	// Its options, and the names of the server's credentials, which are
	// placeholders, which it looks up
	u, err = volume("media", "media", "--endpoint", "http://localhost:9000", "--region", "eu-west-1",
		"--access-key-credential", "S3_KEY", "--secret-key-credential", "S3_SECRET", "--readonly",
		"--seaweedfs-filer-grpc", "192.168.86.21:18888")
	require.NoError(t, err)
	assert.Equal(t, "s3://media?access-key-id=%24%7BS3_KEY%7D&endpoint=http%3A%2F%2Flocalhost%3A9000&readonly=true&region=eu-west-1&seaweedfs-filer-grpc=192.168.86.21%3A18888&secret-access-key=%24%7BS3_SECRET%7D", u)

	// A name of a credential which isn't one, such as a secret
	_, err = volume("media", "media", "--secret-key-credential", "not/a-name")
	assert.Error(t, err)
	u, err = volume("public", "public", "--anonymous", "--profile", "work")
	require.NoError(t, err)
	assert.Equal(t, "s3://public?anonymous=true&profile=work", u)

	// A bucket which is empty, and a name which isn't an identifier
	_, err = volume("media", "/")
	assert.Error(t, err)
	_, err = volume("not-a-name!", "media")
	assert.Error(t, err)
}

func Test_VolumeCreateGTasksCmd(t *testing.T) {
	var cli struct {
		VolumeCreateGTasks VolumeCreateGTasksCmd `cmd:"" name:"volume-create-gtasks"`
	}
	parser, err := kong.New(&cli)
	require.NoError(t, err)
	volume := func(args ...string) (string, error) {
		t.Helper()
		cli.VolumeCreateGTasks = VolumeCreateGTasksCmd{}
		_, err := parser.Parse(append([]string{"volume-create-gtasks"}, args...))
		require.NoError(t, err)
		v, err := cli.VolumeCreateGTasks.volume()
		if err != nil {
			return "", err
		}
		return v.URL, nil
	}

	// A volume of the default sign-in, which is read-only, and one which
	// isn't, of another, with a quota and refresh interval
	u, err := volume("tasks")
	require.NoError(t, err)
	assert.Equal(t, "gtasks://tasks", u)
	u, err = volume("work", "--writable", "--credential", "GOOGLE_WORK", "--quota", "60", "--refresh-interval", "5m", "--hidden")
	require.NoError(t, err)
	assert.Equal(t, "gtasks://work?credential=GOOGLE_WORK&hidden=true&quota=60&readonly=false&refresh_interval=5m0s", u)

	// Another option, which no flag sets, such as index, and one which
	// replaces a flag's, and an option without a key isn't one
	u, err = volume("tasks", "--quota", "60", "--opt", "index=folders", "-o", "quota=30")
	require.NoError(t, err)
	assert.Equal(t, "gtasks://tasks?index=folders&quota=30", u)
	_, err = volume("tasks", "-o", "=folders")
	assert.Error(t, err)

	// A name which isn't an identifier, or a credential which isn't a key
	_, err = volume("not-a-name!")
	assert.Error(t, err)
	_, err = volume("tasks", "--credential", "not a key")
	assert.Error(t, err)
}

func Test_VolumeCreateGContactsCmd(t *testing.T) {
	var cli struct {
		VolumeCreateGContacts VolumeCreateGContactsCmd `cmd:"" name:"volume-create-gcontacts"`
	}
	parser, err := kong.New(&cli)
	require.NoError(t, err)
	volume := func(args ...string) (string, error) {
		_, err := parser.Parse(append([]string{"volume-create-gcontacts"}, args...))
		if err != nil {
			return "", err
		}
		v, err := cli.VolumeCreateGContacts.volume()
		return v.URL, err
	}

	// A volume of contacts, and of another sign-in, with a quota, a refresh
	// interval, and another option
	u, err := volume("contacts")
	require.NoError(t, err)
	assert.Equal(t, "gcontacts://contacts", u)
	u, err = volume("work", "--credential", "GOOGLE_WORK", "--quota", "60", "--refresh-interval", "5m", "-o", "index=folders")
	require.NoError(t, err)
	assert.Equal(t, "gcontacts://work?credential=GOOGLE_WORK&index=folders&quota=60&refresh_interval=5m0s", u)

	// A name which isn't an identifier, or a credential which isn't a key
	_, err = volume("not-a-name!")
	assert.Error(t, err)
	_, err = volume("contacts", "--credential", "not a key")
	assert.Error(t, err)
}

func Test_VolumeDescription(t *testing.T) {
	var cli struct {
		VolumeCreate         VolumeCreateCmd          `cmd:"" name:"volume-create"`
		VolumeCreateFS       VolumeCreateFSCmd        `cmd:"" name:"volume-create-fs"`
		VolumeCreateS3       VolumeCreateS3Cmd        `cmd:"" name:"volume-create-s3"`
		VolumeCreateGTasks   VolumeCreateGTasksCmd    `cmd:"" name:"volume-create-gtasks"`
		VolumeCreateGContact VolumeCreateGContactsCmd `cmd:"" name:"volume-create-gcontacts"`
		VolumeUpdate         VolumeUpdateCmd          `cmd:"" name:"volume-update"`
	}
	description := func(rawURL string) string {
		t.Helper()
		u, err := url.Parse(rawURL)
		require.NoError(t, err)
		return u.Query().Get("description")
	}
	parse := func(args ...string) {
		t.Helper()
		parser, err := kong.New(&cli)
		require.NoError(t, err)
		_, err = parser.Parse(args)
		require.NoError(t, err)
	}

	// Each command which creates a volume sets its description, which an
	// option replaces
	parse("volume-create", "file://media/mnt/media?description=Old", "--description", "Family photos")
	v, err := cli.VolumeCreate.volume()
	require.NoError(t, err)
	assert.Equal(t, "Family photos", description(v.URL))
	parse("volume-create", "file://media/mnt/media", "--description", "Family photos", "-o", "description=Other")
	v, err = cli.VolumeCreate.volume()
	require.NoError(t, err)
	assert.Equal(t, "Other", description(v.URL))
	parse("volume-create-fs", "media", "/mnt/media", "--description", "Family photos")
	v, err = cli.VolumeCreateFS.volume()
	require.NoError(t, err)
	assert.Equal(t, "Family photos", description(v.URL))
	parse("volume-create-s3", "photos", "bucket", "--description", "Family photos")
	v, err = cli.VolumeCreateS3.volume()
	require.NoError(t, err)
	assert.Equal(t, "Family photos", description(v.URL))
	parse("volume-create-gtasks", "tasks", "--description", "Shopping")
	v, err = cli.VolumeCreateGTasks.volume()
	require.NoError(t, err)
	assert.Equal(t, "Shopping", description(v.URL))
	parse("volume-create-gcontacts", "contacts", "--description", "Friends")
	v, err = cli.VolumeCreateGContact.volume()
	require.NoError(t, err)
	assert.Equal(t, "Friends", description(v.URL))

	// Updating a volume sets it, or the backend's, when it's empty, but not
	// with an option too, and without the flag, it's kept
	update := func(args ...string) (schema.VolumePatch, error) {
		t.Helper()
		cli.VolumeUpdate = VolumeUpdateCmd{}
		parse(append([]string{"volume-update", "media"}, args...)...)
		return cli.VolumeUpdate.patch()
	}
	patch, err := update("--description", "Family photos")
	require.NoError(t, err)
	assert.JSONEq(t, `["Family photos"]`, string(patch["description"]))
	patch, err = update("--description", "")
	require.NoError(t, err)
	assert.JSONEq(t, `[""]`, string(patch["description"]), "an empty one is the backend's")
	patch, err = update()
	require.NoError(t, err)
	assert.NotContains(t, patch, "description")
	for _, value := range []string{"Family photos", ""} {
		_, err = update("--description", value, "-o", "description=Other")
		assert.ErrorIs(t, err, gofiler.ErrBadParameter, value)
	}
}

func Test_VolumeCreateGCalendarCmd(t *testing.T) {
	var cli struct {
		VolumeCreateGCalendar VolumeCreateGCalendarCmd `cmd:"" name:"volume-create-gcalendar"`
	}
	parser, err := kong.New(&cli)
	require.NoError(t, err)
	volume := func(args ...string) (string, error) {
		_, err := parser.Parse(append([]string{"volume-create-gcalendar"}, args...))
		if err != nil {
			return "", err
		}
		v, err := cli.VolumeCreateGCalendar.volume()
		return v.URL, err
	}

	// A volume of calendars, and of another sign-in, of a window, a quota,
	// and another option
	u, err := volume("calendar")
	require.NoError(t, err)
	assert.Equal(t, "gcalendar://calendar", u)
	u, err = volume("work", "--credential", "GOOGLE_WORK", "--window", "30d", "--quota", "60", "-o", "index=metadata")
	require.NoError(t, err)
	assert.Equal(t, "gcalendar://work?credential=GOOGLE_WORK&index=metadata&quota=60&window=30d", u)

	// A name which isn't an identifier, or a credential which isn't a key
	_, err = volume("not-a-name!")
	assert.Error(t, err)
	_, err = volume("work", "--credential", "not a key")
	assert.Error(t, err)
}

// A volume of a backend which the server doesn't run isn't created, with
// those it does run, before it's created, such as before signing in
func Test_CheckBackend(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/volume", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(schema.VolumeList{Schemes: []string{"file", "s3"}})
	}))
	defer server.Close()
	client, err := httpclient.New(server.URL)
	require.NoError(t, err)
	assert.NoError(t, checkBackend(context.Background(), client, "s3://bucket"))
	err = checkBackend(context.Background(), client, "gtasks://tasks")
	assert.ErrorIs(t, err, gofiler.ErrBadParameter)
	assert.ErrorContains(t, err, `"gtasks"`)
	assert.ErrorContains(t, err, "file, s3")
}
