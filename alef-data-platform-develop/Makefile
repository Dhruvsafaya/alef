#!make
LOCAL_ENV_DIR = local-env

sbt-cmd = @sbt -v -J-Xmx2048m -Duser.timezone=UTC

test:
	@echo "Running sbt test command"
	$(sbt-cmd) test

test-core:
	@echo "Running sbt test command for batch-events"
	$(sbt-cmd) "project core" test

test-batch:
	@echo "Running sbt test command for batch-events"
	$(sbt-cmd) "project batch-events" test

test-ccl-streaming:
	@echo "Running sbt test command for ccl-streaming-events"
	$(sbt-cmd) "project ccl-streaming-events" test

test-streaming:
	@echo "Running sbt test command for streaming-events"
	$(sbt-cmd) "project streaming-events" test

local:
	@echo "Starting dockerized local environment.."
	@docker compose --project-directory ${LOCAL_ENV_DIR} --env-file ${LOCAL_ENV_DIR}/.env up
	@echo "Creating alef-bigdata-emr bucket"
	@aws --endpoint-url=http://localhost:4566 s3 mb  s3://alef-bigdata-emr
