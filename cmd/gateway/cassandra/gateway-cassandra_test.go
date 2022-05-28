package cassandra

import (
	"fmt"
	"github.com/gocql/gocql"
	"testing"
)

func TestCassandra(t *testing.T) {
	cluster := gocql.NewCluster("10.112.186.147")
	//cluster.Keyspace = "store"
	session, err := cluster.CreateSession()
	if err != nil {
		t.Error(err)
	}
	//var text string
	iter := session.Query("SELECT writetime(firstname) FROM system_schema.columns WHERE keyspace_name = 'store' AND table_name = 'shopping_cart';").Iter()
	m, _ := iter.SliceMap()
	fmt.Println(len(m))
	for _, v := range m {
		fmt.Println(v)
		fmt.Println(string(v["column_name_bytes"].([]byte)))
	}
}
