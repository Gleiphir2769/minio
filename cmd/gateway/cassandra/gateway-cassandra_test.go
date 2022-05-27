package cassandra

import (
	"fmt"
	"github.com/gocql/gocql"
	"testing"
)

func TestCassandra(t *testing.T) {
	cluster := gocql.NewCluster("192.168.1.1", "192.168.1.2", "192.168.1.3")
	cluster.Keyspace = "store"
	session, err := cluster.CreateSession()
	if err != nil {
		t.Error(err)
	}
	q := session.Query("list tables")
	fmt.Printf(q.String())
}
