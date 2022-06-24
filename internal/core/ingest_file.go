package core

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"io"
	"os"

	"github.com/segmentio/kafka-go"
)

type IngestFileConfig struct {
	KafkaWriter  *kafka.Writer
	TrackingUuid string
	Logger       *zap.SugaredLogger
	Region       string
}

func GetHeaderIndexes(expectedHeaders []string, headers []string) (map[string]int, error) {
	// Create index header map using the expected headers
	headerIndexes := map[string]int{}
	for _, header := range expectedHeaders {
		headerIndexes[header] = -1
	}
	// Set headers to the correct index using the header row
	for i, header := range headers {
		_, exists := headerIndexes[header] // check existence so we can ignore extra headers
		if exists {
			headerIndexes[header] = i
		}
	}
	// If any expected headers have no index, return error
	for header, index := range headerIndexes {
		if index == -1 {
			return nil, errors.New(fmt.Sprint("missing header ", header, " in headers: ", headers))
		}
	}
	return headerIndexes, nil
}

func (op Operation) IngestFile(ctx context.Context, fileRows chan []string, headerIndexes map[string]int, config IngestFileConfig) {
	logger := config.Logger
	for row := range fileRows {
		// Serialise row using schema
		recordValue, err := op.SerializeRow(row, config.TrackingUuid, op.SchemaID, headerIndexes, config.Region)
		if err != nil {
			logger.Error("Error serialising record to bytes", err)
			continue
		}
		// Put the row on the topic
		err = config.KafkaWriter.WriteMessages(
			ctx,
			kafka.Message{
				Key:   []byte(op.Key),
				Value: recordValue,
			},
		)
		if err != nil {
			logger.Info("could not write message " + err.Error())
			continue
		}
	}
}

func ReadRows(ctx context.Context, logger *zap.SugaredLogger, f *os.File, fileType string, fileRows chan []string) {
	defer f.Close()
	// Use different reader depending on filetype
	var reader Reader
	switch fileType {
	default:
		reader = csv.NewReader(f)
	}
	// Read rows and pass to fileRows channel
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			logger.Error(ctx, "Error reading from file", err)
			continue
		}
		fileRows <- row
	}
	close(fileRows)
}
