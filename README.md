# News Recommender System

A prototype big data news recommendation system built with FastAPI, PostgreSQL, Redis, OpenSearch, and MLflow.

## Quick Start

1. Clone the repository
2. Copy `.env.example` to `.env` and configure
3. Start services: `docker-compose up --build`
4. Access API at: http://localhost:8000
5. View API docs at: http://localhost:8000/docs

## Services

- **API**: http://localhost:8000
- **MLflow**: http://localhost:5000
- **OpenSearch**: http://localhost:9200
- **PostgreSQL**: localhost:5432 (use a database client, not a browser)
- **Redis**: localhost:6379 (use a Redis client, not a browser)

## Development


## System Verification & Service Roles

### 1. Check Container Health
Run:
```bash
docker-compose ps
```
All services should show "Up" or "healthy".

### 2. Check Logs for Errors
For each service, run:
```bash
docker-compose logs <service>
```
Example:
```bash
docker-compose logs api
docker-compose logs postgres
docker-compose logs redis
docker-compose logs opensearch
docker-compose logs mlflow
docker-compose logs kafka
docker-compose logs spark-master
docker-compose logs spark-worker
```

### 3. Verify API Service
Test endpoints with curl or Postman:
```bash
curl http://localhost:8000/health
curl http://localhost:8000/users/
curl http://localhost:8000/articles/
curl http://localhost:8000/recommendations/
```

### 4. Verify PostgreSQL
Connect using a database client (e.g., DBeaver, PgAdmin, or psql):
- Host: `localhost`
- Port: `5432`
- User: `newsuser`
- Password: `newspass`
- Database: `newsdb`

### 5. Verify Redis
Use a Redis client or CLI:
```bash
redis-cli -h localhost -p 6379 ping
```
Should return `PONG`.

### 6. Verify OpenSearch
Test with curl:
```bash
curl http://localhost:9200
```
Should return OpenSearch info in JSON.

### 7. Verify MLflow
Open in browser:
- http://localhost:5000
You should see the MLflow UI.

### 8. Verify Kafka
Get the Kafka container ID:
```bash
docker ps | grep kafka
```
Use the value in the first column as `<kafka_container_id>`.

Create a topic:
```bash
docker exec -it <kafka_container_id> kafka-topics --create --topic test --bootstrap-server localhost:9092
```
Produce a message:
```bash
docker exec -it <kafka_container_id> kafka-console-producer --topic test --bootstrap-server localhost:9092
```
Consume a message:
```bash
docker exec -it <kafka_container_id> kafka-console-consumer --topic test --bootstrap-server localhost:9092 --from-beginning
```

### 9. Verify Spark
Get the Spark master container ID:
```bash
docker ps | grep spark-master
```
Use the value in the first column as `<spark-master_container_id>`.

Submit a simple job:
```bash
docker exec -it <spark-master_container_id> spark-submit --master spark://spark-master:7077 examples/src/main/python/pi.py 10
```
Should print an estimated value of Pi.

### 10. Service Roles
- **API**: Handles HTTP requests, business logic, and connects to other services.
- **PostgreSQL**: Stores persistent data (users, articles, interactions).
- **Redis**: Caches data for fast access.
- **OpenSearch**: Provides search capabilities for articles.
- **MLflow**: Tracks ML experiments and models.
- **Kafka**: Streams real-time data/events.
- **Spark**: Processes and analyzes large datasets.