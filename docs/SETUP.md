# Setup Instructions

## Prerequisites
- Docker & Docker Compose installed
- Python 3.10+ (for local development)

## Steps
1. Clone the repository:
	```bash
	git clone <repo-url>
	cd news-recommender
	```
2. Copy `.env.example` to `backend/.env` and configure environment variables:
	```bash
	cp .env.example backend/.env
	# Edit backend/.env with your values
	```
3. Build and start all services:
	```bash
	docker-compose up --build
	```
4. Check service health:
	```bash
	docker-compose ps
	docker-compose logs api
	```
5. Test API endpoints:
	```bash
	curl http://localhost:8000/health
	curl http://localhost:8000/articles/
	```

## Environment Variables
Edit `backend/.env` with values for:
- `DATABASE_URL`
- `REDIS_URL`
- `OPENSEARCH_URL`
- `MLFLOW_URL`
- `KAFKA_BOOTSTRAP_SERVERS`

## Troubleshooting
- Use `docker-compose logs <service>` to debug issues.
- Make sure all containers are "Up" or "healthy".
