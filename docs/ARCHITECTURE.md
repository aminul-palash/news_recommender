# System Architecture

## Overview
The News Recommender system is a microservices-based big data platform for ingesting, storing, searching, and recommending news articles to users.

## Components
- **API (FastAPI)**: Entry point for clients, exposes REST endpoints.
- **PostgreSQL**: Relational database for persistent storage.
- **Redis**: In-memory cache for fast access.
- **OpenSearch**: Full-text search engine for articles.
- **MLflow**: ML experiment tracking and model registry.
- **Kafka**: Real-time event streaming (news ingestion, user events).
- **Spark**: Distributed data processing and analytics.

## Data Flow
1. News articles and user events are ingested via API or Kafka.
2. Data is stored in PostgreSQL and indexed in OpenSearch.
3. Spark jobs process data for analytics and recommendations.
4. ML models are trained and tracked in MLflow.
5. Redis caches recommendations and frequently accessed data.
6. API serves recommendations and search results to users.

## Diagram

```
User/Client
	|
	v
[API (FastAPI)] <--> [PostgreSQL] <--> [Redis]
	|                |
	|                v
	|            [Kafka] <--> [Spark]
	|                |
	v                v
[OpenSearch]    [MLflow]
```

## Scaling
- All services are containerized and can be scaled independently.
- Kafka and Spark enable real-time and batch big data processing.
