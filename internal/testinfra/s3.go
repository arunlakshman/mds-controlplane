//go:build integration

package testinfra

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var minioSvc service

const (
	minioUser = "minioadmin"
	minioPass = "minioadmin"
)

// S3Info says how to reach one fresh MinIO bucket.
type S3Info struct {
	Endpoint string     // http://host:port, for s3store.Config.Endpoint and -s3-endpoint
	Region   string     // us-east-1
	Bucket   string     // new, empty
	Client   *s3.Client // ready to use, path-style, against Endpoint
}

// S3 starts (once per package) a MinIO container and returns a new empty
// bucket. MinIO needs real credentials, so S3 sets AWS_ACCESS_KEY_ID and
// AWS_SECRET_ACCESS_KEY for the test (with t.Setenv: do not use t.Parallel).
// s3store.New then picks them up. Use PathStyle: true. The bucket is deleted
// when the test ends. It fails the test if Docker is not running.
func S3(t testing.TB) S3Info {
	t.Helper()
	addr, err := minioSvc.start(testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Image:        MinIOImage,
		ExposedPorts: []string{"9000/tcp"},
		Env:          map[string]string{"MINIO_ROOT_USER": minioUser, "MINIO_ROOT_PASSWORD": minioPass},
		WaitingFor:   wait.ForHTTP("/minio/health/ready").WithPort("9000/tcp"),
	}}, "9000/tcp")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("AWS_ACCESS_KEY_ID", minioUser)
	t.Setenv("AWS_SECRET_ACCESS_KEY", minioPass)
	endpoint := "http://" + addr
	client := s3.New(s3.Options{
		Region:                     "us-east-1",
		BaseEndpoint:               aws.String(endpoint),
		UsePathStyle:               true,
		Credentials:                credentials.NewStaticCredentialsProvider(minioUser, minioPass, ""),
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	})
	// Bucket names allow lower case letters, digits and "-", not "_".
	bucket := strings.ReplaceAll(unique("bucket"), "_", "-")
	if _, err := client.CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: &bucket}); err != nil {
		t.Fatalf("testinfra: create bucket %s: %v", bucket, err)
	}
	t.Cleanup(func() { emptyAndDelete(client, bucket) })
	return S3Info{Endpoint: endpoint, Region: "us-east-1", Bucket: bucket, Client: client}
}

func emptyAndDelete(client *s3.Client, bucket string) {
	ctx := context.Background()
	p := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{Bucket: &bucket})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return
		}
		for _, o := range page.Contents {
			_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: o.Key})
		}
	}
	_, _ = client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: &bucket})
}
