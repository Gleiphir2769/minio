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
	//iter := session.Query("CREATE TABLE IF NOT EXISTS minio.managed_buckets (\nid text PRIMARY KEY,\ncreate_timestamp timestamp\n);").Iter()
	//m, _ := iter.SliceMap()
	//fmt.Println(len(m))
	//for _, v := range m {
	//	fmt.Println(v)
	//	fmt.Println(string(v["column_name_bytes"].([]byte)))
	//}
	q := session.Query("CREATE TABLE minio.managed_buckets (\nid text2 PRIMARY KEY,\ncreate_timestamp timestamp\n);")
	err = q.Exec()
	switch err.(type) {
	case *gocql.RequestErrAlreadyExists:
		fmt.Println("catched error")
	default:
		fmt.Println("not catched")
	}
}

func TestOthers(t *testing.T) {
	fmt.Println(isBucketReserved("managed_buckets"))
}

func TestRegisterBucket(t *testing.T) {
	co, _ := mockCassandraObject()
	err := co.registerBucket("shenjiaqi")
	if err != nil {
		t.Error(err)
	}
}

func TestDeregisterBucket(t *testing.T) {
	co, _ := mockCassandraObject()
	err := co.deregisterBucket("shenjiaqi")
	if err != nil {
		t.Error(err)
	}
}

func TestCassandraObjects_DeleteBucket(t *testing.T) {
	co, _ := mockCassandraObject()
	err := co.deleteBucket("shenjiaqi")
	if err != nil {
		t.Error(err)
	}
}

func TestCassandraObjects_CreateBucket(t *testing.T) {
	co, _ := mockCassandraObject()
	err := co.createBucket("shenjiaqi")
	if err != nil {
		t.Error(err)
	}
}

func TestCassandraObjects_GetBucketCreateTime(t *testing.T) {
	co, _ := mockCassandraObject()
	ti, err := co.getBucketCreateTime("shenjiaqi")
	if err != nil {
		t.Error(err)
	} else {
		fmt.Println(ti.String())
	}
}

func TestInsertToBucket(t *testing.T) {
	co := &cassandraObjects{}
	co.cluster = gocql.NewCluster("10.112.186.147")
	err := co.insertToBucket("shenjiaqi", "test", []byte("hala madrid"))
	if err != nil {
		t.Error(err)
	}
}

func TestCassandraObjects_GetObjectInfo(t *testing.T) {
	co, _ := mockCassandraObject()
	objs, err := co.getObjectInfos("shenjiaqi")
	if err != nil {
		t.Error(err)
	}
	fmt.Println(len(objs))
	for _, obj := range objs {
		fmt.Println(obj)
	}
}

func mockCassandraObject() (*cassandraObjects, error) {
	co := &cassandraObjects{}
	co.cluster = gocql.NewCluster("10.112.186.147")
	err := co.createManagedBucketsTable()
	if err != nil {
		return nil, err
	}
	return co, nil
}
