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
	DefaultSchema            = "queue"
	QueueListLimit           = 100
	TickerListLimit          = 100
	DefaultTickerPeriod      = 5 * time.Second
	DefaultMaintenancePeriod = 1 * time.Hour
	DefaultPartitionSize     = 100_000 // tasks per partition
	DefaultPartitionAhead    = 2       // how many partitions to keep pre-created
)
