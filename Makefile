.PHONY: up down logs test chaos calm dlq

up:            ## start the whole stack
	docker compose up -d --build

down:          ## stop and delete volumes
	docker compose down -v

logs:
	docker compose logs -f producer spark-job alert-service

test:          ## unit + Spark tests (needs Java 17+ locally)
	python -m pytest -q

chaos:         ## make the case API fail 60% of the time -> watch retries, breaker, DLQ
	SINK_FAILURE_RATE=0.6 docker compose up -d alert-service

calm:
	SINK_FAILURE_RATE=0.05 docker compose up -d alert-service

dlq:           ## peek at dead-lettered alerts
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
	  --topic fraud-alerts-dlq --from-beginning --property print.headers=true --timeout-ms 5000
