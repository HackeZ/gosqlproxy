package conf

import (
	"testing"
)

func TestSchema_DSN(t *testing.T) {
	cases := []struct {
		sch Schema
		dsn string
	}{
		{
			sch: Schema{
				DBName: "student",
			},
			dsn: "/student",
		},
		{
			sch: Schema{
				Host:     "localhost",
				Port:     3306,
				User:     "hi",
				Password: "tony",
				DBName:   "school",
				Params:   "param1=value1&param2=value2&param3=value3",
			},
			dsn: "hi:tony@tcp(localhost:6300)/school?param1=value1&param2=value2&param3=value3",
		},
		{
			sch: Schema{
				Host:   "localhost",
				Port:   3306,
				DBName: "school",
			},
			dsn: "tcp(localhost:6300)/school",
		},
	}

	for _, c := range cases {
		if dsn := c.sch.DSN(); c.dsn != dsn {
			t.Fatalf("failed to parse dsn, want: %s, got: %s\n", c.dsn, dsn)
		}
	}
}
