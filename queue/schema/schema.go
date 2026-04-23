package schema

import (
	_ "embed"
	"time"
)

///////////////////////////////////////////////////////////////////////////////
// GLOBALS

//go:embed objects.sql
var Objects string

//go:embed queries.sql
var Queries string

const (
	DefaultSchema       = "queue"
	QueueListLimit      = 100
	TickerListLimit     = 100
	DefaultTickerPeriod = 5 * time.Second
)
