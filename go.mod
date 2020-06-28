module github.com/256dpi/turing

go 1.14

require (
	github.com/256dpi/god v0.4.3
	github.com/cockroachdb/pebble v0.0.0-20200219202912-046831eaec09
	github.com/golang/protobuf v1.3.4 // indirect
	github.com/lni/dragonboat/v3 v3.2.0
	github.com/lni/goutils v1.1.0
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/prometheus/client_golang v1.5.1
	github.com/stretchr/testify v1.4.0
	github.com/tidwall/cast v0.0.0-20160910020434-3045c88cf4cd
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f // indirect
)

replace github.com/cockroachdb/pebble v0.0.0-20200219202912-046831eaec09 => github.com/256dpi/pebble v0.0.0-20200414073916-7b64097a81ce
