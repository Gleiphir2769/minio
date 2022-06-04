package cassandra

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/minio/cli"
	"github.com/minio/minio-go/v7/pkg/s3utils"
	minio "github.com/minio/minio/cmd"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	cassandraSeparator = minio.SlashSeparator
	bucketManager      = "managed_buckets"
	reservedBuckets    = "managed_buckets"
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
	cluster := os.Getenv("CASSANDRA_CLUSTER")
	co := &cassandraObjects{}
	co.cluster = gocql.NewCluster(cluster)
	err := co.createManagedBucketsTable()
	if err != nil {
		return nil, err
	}
	return co, nil
}

func (c *Cassandra) Production() bool {
	return true
}

type cassandraObjects struct {
	minio.GatewayUnsupported
	cluster *gocql.ClusterConfig
	// todo: session池化
	session *gocql.Session
}

func (co *cassandraObjects) Shutdown(ctx context.Context) error {
	return nil
}

func (co *cassandraObjects) StorageInfo(ctx context.Context, _ bool) (si minio.StorageInfo, errs []error) {
	return minio.StorageInfo{}, nil
}

func (co *cassandraObjects) DeleteBucket(ctx context.Context, bucket string, forceDelete bool) error {
	if !cassandraIsValidBucketName(bucket) {
		return minio.BucketNameInvalid{Bucket: bucket}
	}
	return cassandraToObjectErr(ctx, co.deleteBucket(bucket), bucket)
}

func (co *cassandraObjects) MakeBucketWithLocation(ctx context.Context, bucket string, opts minio.BucketOptions) error {
	if opts.LockEnabled || opts.VersioningEnabled {
		return minio.NotImplemented{}
	}
	if !cassandraIsValidBucketName(bucket) {
		return minio.BucketNameInvalid{Bucket: bucket}
	}
	return cassandraToObjectErr(ctx, co.createBucket(bucket), bucket)
}

func (co *cassandraObjects) GetBucketInfo(ctx context.Context, bucket string) (bi minio.BucketInfo, err error) {
	createTime, err := co.getBucketCreateTime(bucket)
	if err != nil {
		return bi, cassandraToObjectErr(ctx, err, bucket)
	}
	// As hdfs.Stat() doesn't carry anything other than ModTime(), use ModTime() as CreatedTime.
	return minio.BucketInfo{
		Name:    bucket,
		Created: *createTime,
	}, nil
}

func (co *cassandraObjects) ListBuckets(ctx context.Context) (buckets []minio.BucketInfo, err error) {
	session, err := co.cluster.CreateSession()
	defer session.Close()
	if err != nil {
		return nil, err
	}
	iter := session.Query("SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'minio';").Iter().Scanner()
	var tableName string
	for iter.Next() {
		_ = iter.Scan(&tableName)
		if isBucketReserved(tableName) {
			continue
		}
		buckets = append(buckets, minio.BucketInfo{
			Name: tableName,
			// As hdfs.Stat() doesnt carry CreatedTime, use ModTime() as CreatedTime.
			Created: time.Now(),
		})
	}
	return buckets, nil
}

func (co *cassandraObjects) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, err error) {
	if len(prefix) > 0 {
		return loi, minio.NotImplemented{API: "Bucket Prefix not Implemented"}
	}
	objectInfos, err := co.getObjectInfos(bucket)
	if err != nil {
		return loi, err
	}
	return minio.ListObjectsInfo{
		IsTruncated: false,
		NextMarker:  "",
		Objects:     objectInfos,
		Prefixes:    []string{},
	}, nil
}

func (co *cassandraObjects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int,
	fetchOwner bool, startAfter string) (loi minio.ListObjectsV2Info, err error) {
	// fetchOwner is not supported and unused.
	marker := continuationToken
	if marker == "" {
		marker = startAfter
	}
	resultV1, err := co.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		return loi, err
	}
	return minio.ListObjectsV2Info{
		Objects:               resultV1.Objects,
		Prefixes:              resultV1.Prefixes,
		ContinuationToken:     continuationToken,
		NextContinuationToken: resultV1.NextMarker,
		IsTruncated:           resultV1.IsTruncated,
	}, nil
}

func (co *cassandraObjects) DeleteObject(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (minio.ObjectInfo, error) {
	err := cassandraToObjectErr(ctx, co.deleteObject(bucket, object), bucket, object)
	return minio.ObjectInfo{
		Bucket: bucket,
		Name:   object,
	}, err
}

func (co *cassandraObjects) DeleteObjects(ctx context.Context, bucket string, objects []minio.ObjectToDelete, opts minio.ObjectOptions) ([]minio.DeletedObject, []error) {
	errs := make([]error, len(objects))
	dobjects := make([]minio.DeletedObject, len(objects))
	for idx, object := range objects {
		_, errs[idx] = co.DeleteObject(ctx, bucket, object.ObjectName, opts)
		if errs[idx] == nil {
			dobjects[idx] = minio.DeletedObject{
				ObjectName: object.ObjectName,
			}
		}
	}
	return nil, nil
}

func (co *cassandraObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (gr *minio.GetObjectReader, err error) {
	objInfo, err := co.GetObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		return nil, err
	}

	var startOffset, length int64
	startOffset, length, err = rs.GetOffsetLength(objInfo.Size)
	if err != nil {
		return nil, err
	}

	pr, pw := io.Pipe()
	go func() {
		nerr := co.GetObject(ctx, bucket, object, startOffset, length, pw, objInfo.ETag, opts)
		pw.CloseWithError(nerr)
	}()

	// Setup cleanup function to cause the above go-routine to
	// exit in case of partial read
	pipeCloser := func() { pr.Close() }
	return minio.NewGetObjectReaderFromReader(pr, objInfo, opts, pipeCloser)
}

func (co *cassandraObjects) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (minio.ObjectInfo, error) {
	if minio.IsStringEqual(srcBucket, dstBucket) && minio.IsStringEqual(srcObject, dstObject) {
		return co.GetObjectInfo(ctx, srcBucket, srcObject, srcOpts)
	}
	return co.PutObject(ctx, dstBucket, dstObject, srcInfo.PutObjReader, minio.ObjectOptions{
		ServerSideEncryption: dstOpts.ServerSideEncryption,
		UserDefined:          srcInfo.UserDefined,
	})
}

func (co *cassandraObjects) GetObject(ctx context.Context, bucket, key string, startOffset, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) error {
	data, err := co.getObjectData(bucket, key)
	if err != nil {
		return cassandraToObjectErr(ctx, err, bucket, key)
	}
	rd := bytes.NewReader(data)
	_, err = io.Copy(writer, io.NewSectionReader(rd, startOffset, length))
	if err == io.EOF {
		err = nil
	}
	return cassandraToObjectErr(ctx, err, bucket, key)
}

func (co *cassandraObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	info, err := co.getObjectInfo(bucket, object)
	if err != nil {
		return objInfo, cassandraToObjectErr(ctx, err, bucket, object)
	}
	return *info, nil
}

func (co *cassandraObjects) PutObject(ctx context.Context, bucket string, object string, r *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	blobs, err := io.ReadAll(r)
	if err != nil {
		return objInfo, cassandraToObjectErr(ctx, err, bucket, object)
	}
	if err = co.insertToBucket(bucket, object, blobs); err != nil {
		return objInfo, cassandraToObjectErr(ctx, err, bucket, object)
	}
	return minio.ObjectInfo{
		Bucket: bucket,
		Name:   object,
		ETag:   r.MD5CurrentHexString(),
		// todo: it's not correct for ModTime and AccTime
		ModTime: time.Now(),
		Size:    int64(len(blobs)),
		IsDir:   false,
		AccTime: time.Now(),
	}, nil
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

func (co *cassandraObjects) createManagedBucketsTable() error {
	s, err := co.cluster.CreateSession()
	if err != nil {
		return err
	}
	defer s.Close()
	if err = s.Query("CREATE KEYSPACE IF NOT EXISTS minio WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };").Exec(); err != nil {
		return err
	}
	if err = s.Query("CREATE TABLE IF NOT EXISTS minio.managed_buckets (bucket_name text PRIMARY KEY, create_timestamp timestamp);").Exec(); err != nil {
		return err
	}
	return nil
}

func (co *cassandraObjects) createBucket(bucket string) error {
	s, err := co.cluster.CreateSession()
	if err != nil {
		return err
	}
	defer s.Close()
	cql := fmt.Sprintf("CREATE TABLE minio.%s (object_name text PRIMARY KEY, uploadid text, data blob, size int, create_timestamp timestamp);", bucket)
	if err = s.Query(cql).Exec(); err != nil {
		return err
	}
	// todo: 需要保证创建和注册的原子性
	return co.registerBucket(bucket)
}

func (co *cassandraObjects) registerBucket(bucket string) error {
	s, err := co.cluster.CreateSession()
	if err != nil {
		return err
	}
	defer s.Close()
	cql := fmt.Sprintf("INSERT INTO minio.%s (bucket_name, create_timestamp) VALUES(?, ?);", bucketManager)
	if err = s.Query(cql, bucket, time.Now()).Exec(); err != nil {
		return err
	}
	return nil
}

func (co *cassandraObjects) deregisterBucket(bucket string) error {
	s, err := co.cluster.CreateSession()
	if err != nil {
		return err
	}
	defer s.Close()
	cql := fmt.Sprintf("DELETE FROM minio.%s where bucket_name=?;", bucketManager)
	if err = s.Query(cql, bucket).Exec(); err != nil {
		return err
	}
	return nil
}

func (co *cassandraObjects) deleteBucket(bucket string) error {
	s, err := co.cluster.CreateSession()
	if err != nil {
		return err
	}
	defer s.Close()
	cql := fmt.Sprintf("DROP TABLE minio.%s", bucket)
	if err = s.Query(cql).Exec(); err != nil {
		return err
	}
	return co.deregisterBucket(bucket)
}

func (co *cassandraObjects) getBucketCreateTime(bucket string) (*time.Time, error) {
	s, err := co.cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	defer s.Close()
	var createTime time.Time
	cql := fmt.Sprintf("SELECT create_timestamp FROM minio.%s where bucket_name=? LIMIT 1", bucketManager)
	if err = s.Query(cql, bucket).
		Consistency(gocql.One).Scan(&createTime); err != nil {
		return nil, err
	}
	return &createTime, nil
}

func (co *cassandraObjects) insertToBucket(bucket, object string, blobs []byte) error {
	s, err := co.cluster.CreateSession()
	if err != nil {
		return err
	}
	defer s.Close()
	cql := fmt.Sprintf("INSERT INTO minio.%s (object_name, create_timestamp, data, size, uploadid) VALUES (?, ?, ?, ?, ?);", bucket)
	if err = s.Query(cql, object, time.Now(), typeAsBlobs(blobs), len(blobs), minio.MustGetUUID()).Exec(); err != nil {
		return err
	}
	return nil
}

func (co *cassandraObjects) getObjectInfos(bucket string) ([]minio.ObjectInfo, error) {
	s, err := co.cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	defer s.Close()
	var name string
	var createTime time.Time
	var size int64
	var uploadId string
	cql := fmt.Sprintf("SELECT object_name, create_timestamp, size, uploadId FROM minio.%s;", bucket)
	iter := s.Query(cql).Iter().Scanner()
	objectInfos := make([]minio.ObjectInfo, 0)
	for iter.Next() {
		err = iter.Scan(&name, &createTime, &size, &uploadId)
		if err != nil {
			return nil, err
		}
		objectInfos = append(objectInfos, minio.ObjectInfo{
			Bucket:  bucket,
			Name:    name,
			ModTime: createTime,
			Size:    size,
			IsDir:   false,
			AccTime: createTime,
		})
	}
	return objectInfos, nil
}

func (co *cassandraObjects) getObjectInfo(bucket string, object string) (*minio.ObjectInfo, error) {
	s, err := co.cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	defer s.Close()
	var name string
	var createTime time.Time
	var size int64
	var uploadId string
	cql := fmt.Sprintf("SELECT object_name, create_timestamp, size, uploadId FROM minio.%s where object_name=? LIMIT 1;", bucket)
	if err = s.Query(cql, object).Consistency(gocql.One).Scan(&name, &createTime, &size, &uploadId); err != nil {
		return nil, err
	}
	return &minio.ObjectInfo{
		Bucket:  bucket,
		Name:    name,
		ModTime: createTime,
		Size:    size,
		IsDir:   false,
		AccTime: createTime,
	}, nil
}

func (co *cassandraObjects) getObjectData(bucket string, object string) ([]byte, error) {
	s, err := co.cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	defer s.Close()
	var rawBlobs string
	cql := fmt.Sprintf("SELECT data FROM minio.%s where object_name=? LIMIT 1;", bucket)
	if err = s.Query(cql, object).Consistency(gocql.One).Scan(&rawBlobs); err != nil {
		return nil, err
	}
	return hex.DecodeString(rawBlobs)
}

func (co *cassandraObjects) deleteObject(bucket string, object string) error {
	s, err := co.cluster.CreateSession()
	if err != nil {
		return err
	}
	defer s.Close()
	cql := fmt.Sprintf("DELETE FROM minio.%s where object_name=?;", bucket)
	if err = s.Query(cql, object).Exec(); err != nil {
		return err
	}
	return nil
}

func isBucketReserved(bucket string) bool {
	return strings.Contains(reservedBuckets, bucket)
}

// cassandraIsValidBucketName verifies whether a bucket name is valid.
func cassandraIsValidBucketName(bucket string) bool {
	return s3utils.CheckValidBucketNameStrict(bucket) == nil
}

func typeAsBlobs(blobs []byte) string {
	return hex.EncodeToString(blobs)
}

func cassandraToObjectErr(ctx context.Context, err error, params ...string) error {
	if err == nil {
		return nil
	}
	bucket := ""
	object := ""
	uploadID := ""
	switch len(params) {
	case 3:
		uploadID = params[2]
		fallthrough
	case 2:
		object = params[1]
		fallthrough
	case 1:
		bucket = params[0]
	}

	switch err.(type) {
	case *gocql.RequestErrUnavailable:
		if uploadID != "" {
			return minio.InvalidUploadID{
				UploadID: uploadID,
			}
		}
		if object != "" {
			return minio.ObjectNotFound{Bucket: bucket, Object: object}
		}
		return minio.BucketNotFound{Bucket: bucket}
	case *gocql.RequestErrAlreadyExists:
		if object != "" {
			return minio.PrefixAccessDenied{Bucket: bucket, Object: object}
		}
		return minio.BucketAlreadyOwnedByYou{Bucket: bucket}
	case *WaitToImplementError:
		if object != "" {
			return minio.PrefixAccessDenied{Bucket: bucket, Object: object}
		}
		return minio.BucketNotEmpty{Bucket: bucket}
	default:
		logger.LogIf(ctx, err)
		return err
	}
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

type WaitToImplementError struct {
}

func (e *WaitToImplementError) Error() string {
	panic("wait to implement")
	return "wait to implement"
}
