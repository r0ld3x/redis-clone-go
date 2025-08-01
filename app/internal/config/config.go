package config

import (
	"flag"
	"fmt"
	"strings"
)

type Config struct {
	Directory     string
	DBFileName    string
	HostName      string
	Port          string
	Role          string
	MasterAddress string
}

func LoadConfig() *Config {
	dir := flag.String("dir", "", "Directory to store the database")
	dbfilename := flag.String("dbfilename", "", "Database file name")
	port := flag.Int("port", 6379, "Port to run the server on")
	replicaof := flag.String("replicaof", "", "Master address if this is a replica (format: host port)")

	flag.Parse()

	config := &Config{
		Directory:  *dir,
		DBFileName: *dbfilename,
		HostName:   "localhost",
		Port:       fmt.Sprintf("%d", *port),
		Role:       "master",
	}

	if *replicaof != "" {
		parts := strings.Fields(*replicaof)
		if len(parts) != 2 {
			panic("Invalid --replicaof format, expected: <host> <port>")
		}
		config.Role = "slave"
		config.MasterAddress = fmt.Sprintf("%s:%s", parts[0], parts[1])
	}

	return config
}

func (c *Config) IsMaster() bool {
	return c.Role == "master"
}

func (c *Config) IsSlave() bool {
	return c.Role == "slave"
}

func (c *Config) GetListenAddress() string {
	return fmt.Sprintf("%s:%s", c.HostName, c.Port)
}
