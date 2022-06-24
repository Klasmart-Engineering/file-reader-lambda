package e2e

import (
	"bytes"
	"context"
	"strings"
	"testing"

	avro "github.com/KL-Engineering/file-reader-lambda/api/avro/avro_gencode"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

func TestUploadFile(t *testing.T) {
	uploadOrgWith(t, 5)
}

func uploadOrgWith(t *testing.T, numberOfRows int) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"localhost:9092"},
		GroupID:     "consumer-group-" + uuid.NewString(),
		Topic:       "organization-avro",
		StartOffset: kafka.FirstOffset,
	})

	content := "uuid,name,owner_user_uuid\n"
	var orgs []OrgRow
	for i := 0; i < numberOfRows; i++ {
		orgs = append(orgs, randomOrgRow())
	}

	for i := 0; i < len(orgs); i++ {
		content = content + orgs[i].rowString() + "\n"
	}

	key := uuid.NewString() + ".csv"
	bucket := "organization"

	result, err := s3Client(t).PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(key),
		Body:          strings.NewReader(content),
		ContentLength: int64(len(content)),
		ContentType:   aws.String("text/csv"),
	})

	assert.Nil(t, err, "error putting s3 object to bucket")

	t.Logf("result %v", result.ETag)

	messageCount := 0
	for i := 0; i < len(orgs); i++ {
		expectedOrg := orgs[i]
		message, err := r.ReadMessage(context.Background())

		assert.Nil(t, err, "error reading message from topic")

		val, err := avro.DeserializeOrganization(bytes.NewReader(message.Value[5:]))

		assert.Nil(t, err, "error deserializing message from topic")

		expectedPayload := avro.NewOrganizationPayload()
		expectedPayload.Name = expectedOrg.Name
		expectedPayload.Uuid = expectedOrg.Uuid
		expectedPayload.Owner_user_uuid = expectedOrg.OwnerUserUuid

		assert.Equal(t, expectedPayload, val.Payload)
		assert.Equal(t, "s3", val.Metadata.Origin_application)
		assert.Equal(t, "eu-west-1", val.Metadata.Region)
		messageCount += 1
	}
	assert.Equal(t, numberOfRows, messageCount)
}

func s3Client(t *testing.T) *s3.Client {
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID: "aws",
			URL:         "http://localhost:4566",
		}, nil
	})

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithEndpointResolverWithOptions(customResolver))

	assert.Nil(t, err, "error creating aws config")

	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})
	return s3Client
}

type OrgRow struct {
	Uuid          string
	Name          string
	OwnerUserUuid string
}

func randomOrgRow() OrgRow {
	return OrgRow{Uuid: uuid.NewString(), Name: uuid.NewString(), OwnerUserUuid: uuid.NewString()}
}

func (orgRow *OrgRow) rowString() string {
	return orgRow.Uuid + "," + orgRow.Name + "," + orgRow.OwnerUserUuid
}
