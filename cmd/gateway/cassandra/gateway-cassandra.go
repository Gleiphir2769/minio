package cassandra

import (
	"context"
	"github.com/gocql/gocql"
	"github.com/minio/cli"
	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/pkg/auth"
	"io"
	"math/rand"
	"net/http"
	"time"
)

const (
	cassandraSeparator = minio.SlashSeparator
)

const letterBytes = "abcdefghijklmnopqrstuvwxyz01234569"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func init() {
	const cassandraGatewayTemplate = `This is a TEST cassandra gateway demo.`

	err := minio.RegisterGatewayCommand(cli.Command{
		Name:               minio.CassandraBackendGateway,
		Usage:              "Amazon Cassandra",
		Action:             cassandraGatewayMain,
		CustomHelpTemplate: cassandraGatewayTemplate,
		HideHelpCommand:    true,
	})
	if err != nil {
		panic(err)
	}
}

// Handler for 'minio gateway hdfs' command line.
func cassandraGatewayMain(ctx *cli.Context) {
	// Validate gateway arguments.
	if ctx.Args().First() == "help" {
		cli.ShowCommandHelpAndExit(ctx, minio.CassandraBackendGateway, 1)
	}
	minio.StartGateway(ctx, &Cassandra{args: ctx.Args()})
}

type Cassandra struct {
	args []string
}

func (c *Cassandra) Name() string {
	return minio.CassandraBackendGateway
}

func (c *Cassandra) NewGatewayLayer(creds auth.Credentials) (minio.ObjectLayer, error) {
	co := &cassandraObjects{}
	co.cluster = gocql.NewCluster("10.112.186.147")
	return co, nil
}

func (c *Cassandra) Production() bool {
	return true
}

type cassandraObjects struct {
	minio.GatewayUnsupported
	cluster *gocql.ClusterConfig
	session *gocql.Session
}

func (co *cassandraObjects) Shutdown(ctx context.Context) error {
	return nil
}

func (co *cassandraObjects) StorageInfo(ctx context.Context, _ bool) (si minio.StorageInfo, errs []error) {
	return minio.StorageInfo{}, nil
}

func (co *cassandraObjects) DeleteBucket(ctx context.Context, bucket string, forceDelete bool) error {
	return nil
}

func (co *cassandraObjects) MakeBucketWithLocation(ctx context.Context, bucket string, opts minio.BucketOptions) error {
	return nil
}

func (co *cassandraObjects) GetBucketInfo(ctx context.Context, bucket string) (bi minio.BucketInfo, err error) {
	return minio.BucketInfo{}, err
}

func (co *cassandraObjects) ListBuckets(ctx context.Context) (buckets []minio.BucketInfo, err error) {
	session, err := co.cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	iter := session.Query("SELECT table_name FROM system_schema.columns WHERE keyspace_name = 'store';").Iter().Scanner()
	var tableName string
	for iter.Next() {
		_ = iter.Scan(&tableName)
		buckets = append(buckets, minio.BucketInfo{
			Name: tableName,
			// As hdfs.Stat() doesnt carry CreatedTime, use ModTime() as CreatedTime.
			Created: time.Now(),
		})
	}
	return buckets, nil
}

func (co *cassandraObjects) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, err error) {

	return loi, err
}

func (co *cassandraObjects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int,
	fetchOwner bool, startAfter string) (loi minio.ListObjectsV2Info, err error) {
	return loi, err
}

func (co *cassandraObjects) DeleteObject(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (minio.ObjectInfo, error) {
	return minio.ObjectInfo{}, nil
}

func (co *cassandraObjects) DeleteObjects(ctx context.Context, bucket string, objects []minio.ObjectToDelete, opts minio.ObjectOptions) ([]minio.DeletedObject, []error) {
	return nil, nil
}

func (co *cassandraObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (gr *minio.GetObjectReader, err error) {
	return nil, err
}

func (co *cassandraObjects) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (minio.ObjectInfo, error) {
	return minio.ObjectInfo{}, nil
}

func (co *cassandraObjects) GetObject(ctx context.Context, bucket, key string, startOffset, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) error {
	return nil
}

func (co *cassandraObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	return minio.ObjectInfo{}, nil
}

func (co *cassandraObjects) PutObject(ctx context.Context, bucket string, object string, r *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	return minio.ObjectInfo{}, nil
}

func (co *cassandraObjects) NewMultipartUpload(ctx context.Context, bucket string, object string, opts minio.ObjectOptions) (uploadID string, err error) {
	return uploadID, nil
}

func (co *cassandraObjects) ListMultipartUploads(ctx context.Context, bucket string, prefix string, keyMarker string, uploadIDMarker string, delimiter string, maxUploads int) (lmi minio.ListMultipartsInfo, err error) {
	return minio.ListMultipartsInfo{}, err
}

func (co *cassandraObjects) GetMultipartInfo(ctx context.Context, bucket, object, uploadID string, opts minio.ObjectOptions) (result minio.MultipartInfo, err error) {
	return result, nil
}

func (co *cassandraObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int, opts minio.ObjectOptions) (result minio.ListPartsInfo, err error) {
	return result, nil
}

func (co *cassandraObjects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject, uploadID string, partID int,
	startOffset int64, length int64, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (minio.PartInfo, error) {
	return minio.PartInfo{}, nil
}

func (co *cassandraObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, r *minio.PutObjReader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
	return info, nil
}

func (co *cassandraObjects) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, parts []minio.CompletePart, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	return minio.ObjectInfo{}, err
}

func (co *cassandraObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, opts minio.ObjectOptions) (err error) {
	return err
}

// randString generates random names and prepends them with a known prefix.
func randString(n int, src rand.Source, prefix string) string {
	b := make([]byte, n)
	// A rand.Int63() generates 63 random bits, enough for letterIdxMax letters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}
	return prefix + string(b[0:30-len(prefix)])
}
