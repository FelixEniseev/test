// try to implement config file
package engine

const (
	DefaultAddr         = "0.0.0.0:49002" // Split to ip and port ???
	DefaultMaxKeySize   = uint32(1 * 1024)
	DefaultMaxValueSize = uint32(8 * 1024)
)

type Config struct {
	Addr             string `json:"addr" toml:"addr"`
	EvictionInterval int    `json:"eviction_interval" toml:"eviction_interval"` // in seconds ???
	// Try to persist
	// Persist bool
	// NoSync bool
	// Path             string `json:"path" toml:"path"`
}
