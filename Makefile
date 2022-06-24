
generate-avro:
	gogen-avro ./api/avro/avro_gencode/ \
	./api/avro/schemas/organization.avsc

build-main:
	@echo "Building main..."
	GOOS=linux CGO_ENABLED=0 go build -ldflags "-s -w" -o main cmd/main.go && zip main.zip main

local-deploy:
	@echo "Deploying locally..."
	docker-compose up -d && \
	sleep 1 && \
	aws --endpoint-url=http://localhost:4566 s3 mb s3://organization && \
	docker exec redpanda-l rpk topic create organization-avro --brokers=localhost:9092 && \
	aws --endpoint-url http://localhost:4566 lambda create-function --timeout 300 --function-name fileevent --handler main --runtime go1.x --role your-role --zip-file fileb://main.zip --environment 'Variables={KAFKA_BROKER=redpanda:29092,AWS_ACCESS_KEY_ID=test,AWS_SECRET_ACCESS_KEY=test,AWS_DEFAULT_REGION=eu-west-1,AWS_ENDPOINT=http://localstack:4566,SCHEMA_CLIENT_ENDPOINT=http://redpanda:8081,ORGANIZATION_AVRO_TOPIC=organization-avro,OPERATIONS="{\"organization\":\"organization_operation\"}"}' && \
	aws --endpoint-url=http://localhost:4566 s3api put-bucket-notification-configuration --bucket organization --notification-configuration file://scripts/local/s3-notif-config.json

local-deploy-clean:
	@echo "Local deploy teardown..."
	aws --endpoint-url=http://localhost:4566 s3 rb s3://organization --force && \
	docker exec redpanda-l rpk topic delete organization-avro --brokers=localhost:9092 && \
	aws --endpoint-url http://localhost:4566 lambda delete-function --function-name fileevent
