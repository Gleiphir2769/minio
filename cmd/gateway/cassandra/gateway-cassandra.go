package cassandra

import (
	"context"
	minio "github.com/minio/minio/cmd"
	"io"
	"net/http"
)

type cassandraObjects struct {
	minio.GatewayUnsupported
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
