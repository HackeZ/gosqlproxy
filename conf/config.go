package conf

import (
	"bytes"
	"fmt"
)

type Config struct {
	SchemaMaster Schema   `json:"master"`
	SchemaSlaves []Schema `json:"salvers"`
}

type Schema struct {
	Host     string `json:"host"`
	Port     int32  `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	DBName   string `json:"db_name"`
	Params   string `json:"params"`

	PacemakerSecond int   `json:"pacemaker_second"`
	Weight          int   `json:"weight"`
	PoolSize        int32 `json:"pool_size"`
}

type SchemaConn interface {
	DSN() string
}

// DSN return data source name to connect mysql
func (s Schema) DSN() string {
	var buf bytes.Buffer

	if s.User != "" {
		buf.WriteString(s.User)
		if s.Password != "" {
			buf.WriteByte(':')
			buf.WriteString(s.Password)
		}

		buf.WriteByte('@')
	}
	if s.Host != "" {
		buf.WriteString("tcp(")
		buf.WriteString(s.Host)
		if s.Port > 0 {
			buf.WriteByte(':')
			buf.WriteString(fmt.Sprintf("%d", s.Port))
		}
		buf.WriteByte(')')
	}
	buf.WriteByte('/')
	buf.WriteString(s.DBName)

	if s.Params != "" {
		buf.WriteByte('?')
		buf.WriteString(s.Params)
	} else {
		buf.WriteString("?charset=utf8mb4&autocommit=true&parseTime=True")
	}

	return buf.String()
}
