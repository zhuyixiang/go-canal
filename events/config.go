package events

type MysqlConfig struct {
	// ServerID is the unique ID in cluster.
	ServerID  uint32
	// Host is for MySQL server host.
	Host      string
	// Port is for MySQL server port.
	Port      uint16
	// User is for MySQL user.
	User      string
	// Password is for MySQL password.
	Password  string
	// Localhost is local hostname if register salve.
	// If not set, use os.Hostname() instead.
	Localhost string
	// Charset is for MySQL client character set
	Charset   string
	//binlog position
	Pos       Position
}


