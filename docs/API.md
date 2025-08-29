# API Documentation

## Overview
This API provides endpoints for user management, news article retrieval, recommendations, and system health checks. It is built with FastAPI and connects to PostgreSQL, Redis, OpenSearch, MLflow, Kafka, and Spark.

## Endpoints

- `GET /health` — Health check for all services
- `POST /users/` — Create a new user
- `GET /users/{user_id}` — Get user details
- `GET /articles/` — List recent articles (optionally filter by category)
- `GET /search/` — Search articles by query and category
- `POST /recommendations/` — Get personalized recommendations for a user
- `GET /admin/stats/` — System statistics
- `POST /test/populate-sample-data/` — Populate database with sample data (dev only)

## Example Request
```bash
curl http://localhost:8000/health
```

## Service Roles
- **API**: Handles HTTP requests and business logic
- **PostgreSQL**: Stores users, articles, interactions
- **Redis**: Caches recommendations and data
- **OpenSearch**: Provides search functionality
- **MLflow**: Tracks ML experiments and models
- **Kafka**: Streams real-time data/events
- **Spark**: Processes and analyzes large datasets
