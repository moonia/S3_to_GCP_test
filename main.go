package main

import (
    "io"
    "fmt"
    "log"
    "os"
    "context"
    "github.com/joho/godotenv"
    "cloud.google.com/go/storage"
    "google.golang.org/api/option"
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/service/s3"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/aws/credentials"
)

func ListObjectsAWS(bucketName string) ([]string, error) {
    godotenv.Load()
    accessKey := os.Getenv("AWS_ACCESS_KEY")
    secretAccessKey := os.Getenv("AWS_SECRET_KEY")

    sess, err := session.NewSession(&aws.Config{
        Region:      aws.String("eu-north-1"),
        Credentials: credentials.NewStaticCredentials(accessKey, secretAccessKey, ""),
    })
    if err != nil {
        return nil, fmt.Errorf("error: AWS connection failed: %v", err)
    }

    cS3 := s3.New(sess)

    result, err := cS3.ListObjectsV2(&s3.ListObjectsV2Input{
        Bucket: aws.String(bucketName),
    })
    if err != nil {
        return nil, fmt.Errorf("error while retrieving S3 objects: %v", err)
    }

    var keys []string
    for _, item := range result.Contents {
        keys = append(keys, *item.Key)
    }

    return keys, nil
}

func S3toGCP(s3BucketName string, gcpBucketName string, objectKey string) error {
    godotenv.Load()
    accessKey := os.Getenv("AWS_ACCESS_KEY")
    secretAccessKey := os.Getenv("AWS_SECRET_KEY")
    path := os.Getenv("PATH_TO_GCP_JSON")

    sess, err := session.NewSession(&aws.Config{
        Region:      aws.String("eu-north-1"),
        Credentials: credentials.NewStaticCredentials(accessKey, secretAccessKey, ""),
    })
    if err != nil {
        return fmt.Errorf("error: AWS connection failed: %v", err)
    }
    cS3 := s3.New(sess)

    resp, err := cS3.GetObject(&s3.GetObjectInput{
        Bucket: aws.String(s3BucketName),
        Key:    aws.String(objectKey),
    })
    if err != nil {
        return fmt.Errorf("error while downloading the S3 object: %v", err)
    }
    defer resp.Body.Close()

    ctx := context.Background()
    
    client, err := storage.NewClient(ctx, option.WithCredentialsFile(path))
    if err != nil {
        return fmt.Errorf("error: GCP connection failed: %v", err)
    }

    bucket := client.Bucket(gcpBucketName)
    object := bucket.Object(objectKey)
    writer := object.NewWriter(ctx)

    if _, err := io.Copy(writer, resp.Body); err != nil {
        return fmt.Errorf("error when uploading to GCP: %v", err)
    }

    if err := writer.Close(); err != nil {
        return fmt.Errorf("error while closing the write stream on GCP: %v", err)
    }

    fmt.Printf("Object %s copied from AWS S3 to GCP\n", objectKey)
    return nil
}

func CreateBucketGCP(bucketName string) {
    godotenv.Load()

    path := os.Getenv("PATH_TO_GCP_JSON")
    ctx := context.Background()
    client, err := storage.NewClient(ctx, option.WithCredentialsFile(path))
    if err != nil {
        log.Fatalf("error: GCP connection failed: %v", err)
    }
    bucket := client.Bucket(bucketName)
    projectID := os.Getenv("PROJECT_ID_GCP")
    if err := bucket.Create(ctx, projectID, nil); err != nil {
        log.Fatalf("error while creating bucket: %v", err)
    }
}

func DeleteBucketGCP(bucketName string) {
    godotenv.Load()

    ctx := context.Background()
    path := os.Getenv("PATH_TO_GCP_JSON")
    client, err := storage.NewClient(ctx, option.WithCredentialsFile(path))

    if err != nil {
        log.Fatalf("error: GCP connection failed: %v", err)
    }
    bucket := client.Bucket(bucketName)
    if err := bucket.Delete(ctx); err != nil {
        log.Fatalf("error while creating bucket: %v", err)
    }
}

func main() {
    gcpBucketName := "test2mooniaGCP"
    s3BucketName := "test2mooniaS3"

    // CreateBucketGCP(gcpBucketName)
    godotenv.Load()

    objectKeys, err := ListObjectsAWS(s3BucketName)
    if err != nil {
        log.Fatalf("error while listing S3 objects: %v", err)
    }

    for _, key := range objectKeys {
        err := S3toGCP(s3BucketName, gcpBucketName, key)
        if err != nil {
            log.Printf("error while copying from %s: %v", key, err)
        }
    }
    // DeleteBucketGCP(gcpBucketName)

    fmt.Println("ok")
}
