package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/KL-Engineering/file-reader-lambda/internal/core"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/google/uuid"
	"github.com/riferrei/srclient"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"log"
	"os"
	"regexp"
	"time"
)

func handler(ctx context.Context, s3Event events.S3Event) {
	deadline, _ := ctx.Deadline()
	deadline = deadline.Add(time.Second * 300)
	logger, err := newLogger()

	if err != nil {
		fmt.Printf("Failed to initialize logger - %v", err)
		panic("Failed to initialize logger")
	}
	var operations map[string]json.RawMessage
	err = json.Unmarshal([]byte(os.Getenv("OPERATIONS")), &operations)
	if err != nil {
		logger.Errorf("Failed to retrieve operation mappings - %v", err)
		panic("Failed to retrieve operation mappings")
	}
	if err != nil {
		logger.Errorf("Failed to get schema - %v", err)
		panic("Failed to get schema")
	}

	schemaRegistryClient := &core.SchemaRegistry{
		Client:      srclient.CreateSchemaRegistryClient(os.Getenv("SCHEMA_CLIENT_ENDPOINT")),
		IdSchemaMap: make(map[int]string),
	}

	ops := core.InitAvroOperations(schemaRegistryClient)

	sess, err := session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Credentials: credentials.NewStaticCredentials(
				os.Getenv("AWS_ACCESS_KEY_ID"),
				os.Getenv("AWS_SECRET_ACCESS_KEY"),
				"",
			),
			Region:           aws.String(os.Getenv("AWS_DEFAULT_REGION")),
			Endpoint:         aws.String(os.Getenv("AWS_ENDPOINT")),
			S3ForcePathStyle: aws.Bool(true),
		},
	})

	for _, record := range s3Event.Records {
		op, exists := ops.GetOperation(record.S3.Bucket.Name)
		if !exists {
			logger.Error("invalid operation_type on file create message ")
			continue
		}

		fileRows := make(chan []string)
		key := record.S3.Object.Key
		err = core.DownloadFile(ctx, logger, sess, core.S3FileCreated{Key: key, BucketName: record.S3.Bucket.Name, ContentType: fileType(key)}, fileRows)
		if err != nil {
			logger.Error(ctx, "error downloading s3 File ", err.Error())
			continue
		}

		// Parse file headers
		headers := <-fileRows
		headerIndexes, err := core.GetHeaderIndexes(op.Headers, headers)
		if err != nil {
			logger.Error(ctx, "Error parsing file headers. ", err.Error())
			continue
		}

		writer := kafka.Writer{
			Addr:                   kafka.TCP(os.Getenv("KAFKA_BROKER")),
			Topic:                  op.Topic,
			Logger:                 log.New(os.Stdout, "kafka writer: ", 0),
			AllowAutoTopicCreation: true,
		}

		op.IngestFile(ctx, fileRows, headerIndexes, core.IngestFileConfig{KafkaWriter: &writer, TrackingUuid: uuid.NewString(), Logger: logger, Region: record.AWSRegion})
	}
}

func newLogger() (*zap.SugaredLogger, error) {
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, err
	}
	logger.Sync()
	sugar := logger.Sugar()
	return sugar, nil
}

func main() {
	lambda.Start(handler)
}

func fileType(key string) string {
	var re = regexp.MustCompile(`(?m)\b[.](?P<filetype>\w+)$`)

	matches := re.FindStringSubmatch(key)
	if matches == nil {
		return key
	}
	fileTypeIndex := re.SubexpIndex("filetype")

	if fileTypeIndex < 0 {
		return key
	}
	return matches[fileTypeIndex]
}
