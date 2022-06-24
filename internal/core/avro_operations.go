package core

import (
	"bytes"
	"encoding/binary"
	avrogen "github.com/KL-Engineering/file-reader-lambda/api/avro/avro_gencode"
	"github.com/KL-Engineering/file-reader-lambda/internal/instrument"
	"io"
	"strings"
)

type avroCodec interface {
	// Represents the structs in the avro-gencode which have Serialize() functions
	Serialize(io.Writer) error
}

type Operations struct {
	OperationMap map[string]Operation
}

type Operation struct {
	Topic        string
	Key          string
	SchemaID     int
	SerializeRow func(row []string, trackingId string, schemaId int, headerIndexes map[string]int, region string) ([]byte, error)
	Headers      []string
}

func (ops Operations) GetOperation(opKey string) (Operation, bool) {
	op, exists := ops.OperationMap[strings.ToUpper(opKey)]
	return op, exists
}

func serializeAvroRecord(codec avroCodec, schemaId int) []byte {
	// Get bytes for the schemaId
	schemaIDBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(schemaIDBytes, uint32(schemaId))

	// Get bytes for the row
	var buf bytes.Buffer
	codec.Serialize(&buf)
	valueBytes := buf.Bytes()

	//Combine row bytes with schema id to make a record
	var recordValue []byte
	recordValue = append(recordValue, byte(0))
	recordValue = append(recordValue, schemaIDBytes...)
	recordValue = append(recordValue, valueBytes...)
	return recordValue
}

const (
	UUID            = "uuid"
	OWNER_USER_UUID = "owner_user_uuid"
	NAME            = "name"
)

var (
	OrganizationHeaders = []string{UUID, NAME, OWNER_USER_UUID}
)

func GetOrganizationSchemaId(schemaRegistryClient *SchemaRegistry, organizationTopic string) int {
	schemaBody := avrogen.Organization.Schema(avrogen.NewOrganization())
	return schemaRegistryClient.GetSchemaId(schemaBody, organizationTopic)
}

func InitAvroOperations(schemaRegistryClient *SchemaRegistry) Operations {
	organizationTopic := instrument.MustGetEnv("ORGANIZATION_AVRO_TOPIC")

	return Operations{
		OperationMap: map[string]Operation{
			"ORGANIZATION": {
				Topic:        organizationTopic,
				Key:          "",
				SchemaID:     GetOrganizationSchemaId(schemaRegistryClient, organizationTopic),
				SerializeRow: RowToOrganizationAvro,
				Headers:      OrganizationHeaders,
			},
		},
	}
}

func RowToOrganizationAvro(row []string, tracking_uuid string, schemaId int, headerIndexes map[string]int, region string) ([]byte, error) {
	// Takes a slice of columns representing an organization and encodes to avro bytes
	md := avrogen.OrganizationMetadata{
		Origin_application: "s3",
		Region:             region,
		Tracking_uuid:      tracking_uuid,
	}
	pl := avrogen.OrganizationPayload{
		Uuid:            row[headerIndexes[UUID]],
		Name:            row[headerIndexes[NAME]],
		Owner_user_uuid: row[headerIndexes[OWNER_USER_UUID]],
	}
	codec := avrogen.Organization{Payload: pl, Metadata: md}
	return serializeAvroRecord(codec, schemaId), nil
}
