package core

import (
	"log"

	"github.com/riferrei/srclient"
)

type SchemaRegistry struct {
	Client      *srclient.SchemaRegistryClient
	IdSchemaMap map[int]string
}

func (SchemaRegistryClient *SchemaRegistry) GetSchemaId(schemaBody string, topic string) int {
	schema, err := SchemaRegistryClient.Client.CreateSchema(topic, schemaBody, "AVRO")
	if err != nil {
		log.Fatal(err)
	}

	return schema.ID()
}

func (SchemaRegistryClient *SchemaRegistry) GetSchema(schemaId int) (string, error) {
	// Gets schema from local cache if exists, otherwise from schema registry
	if _, ok := SchemaRegistryClient.IdSchemaMap[schemaId]; !ok {
		schema, err := SchemaRegistryClient.Client.GetSchema(schemaId)
		if err != nil {
			return "", err
		}
		SchemaRegistryClient.IdSchemaMap[schemaId] = schema.Schema()
	}
	return SchemaRegistryClient.IdSchemaMap[schemaId], nil
}
