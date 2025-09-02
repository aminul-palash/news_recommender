# News Recommender System with Microsoft MIND Dataset

A prototype big data news recommendation system built with FastAPI, PostgreSQL, Redis, OpenSearch, MLflow, Kafka, and Spark, specifically designed to work with the Microsoft MIND (Microsoft News Dataset) for recommendation research.

## Quick Start

### 1. Prerequisites
- Docker & Docker Compose installed
- Python 3.10+ (for local development)
- Microsoft MIND dataset downloaded

### 2. Download MIND Dataset
1. Download the MIND dataset from Microsoft Research:
   - Visit: https://msnews.github.io/
   - Download `MINDlarge_train.zip` or `MINDsmall_train.zip`
2. Extract the dataset to `./data/mind/` directory
3. Ensure you have these files:
   ```
   data/mind/
   ├── news.tsv
   ├── behaviors.tsv
   └── entity_embedding.vec (optional)
   ```

### 3. Setup Environment
```bash
# Clone the repository
git clone <repo-url>
cd news-recommender

# Copy environment configuration
cp .env.example .env

# Edit .env with your configurations
# Set MIND_DATA_DIR=./data/mind
```

### 4. Start All Services

```bash
docker kill $(docker ps -q)
```

```bash
# Build and start all services
docker-compose up --build -d

# Check all services are running
docker-compose ps
```

### 5. Verify Service Health
```bash
# Check API health
curl http://localhost:8000/health

# Check if all containers are healthy
docker-compose logs api
docker-compose logs kafka
docker-compose logs spark-master
```

## Data Ingestion and Processing Pipeline

### Phase 1: Ingest MIND Dataset into Kafka

```bash
# Install dependencies for ingestion script
pip install kafka-python pandas python-dotenv

# Run the ingestion script
python scripts/kafka_producer.py
```

This will:
- Parse `news.tsv` and `behaviors.tsv` files
- Send news articles to `mind-news-articles` topic
- Send user behaviors to `mind-user-behaviors` topic
- Generate user profiles and send to `mind-user-profiles` topic

### Phase 2: Real-time Processing with Kafka Consumers

```bash
# Start the Kafka consumer service (runs automatically with docker-compose)
# Or run manually:
python backend/services/kafka_consumer.py
```

This will:
- Consume news articles and store in PostgreSQL
- Index articles in OpenSearch for search functionality
- Process user behaviors and create interaction records
- Update real-time user metrics in Redis cache

### Phase 3: Batch Processing with Spark

```bash
# Submit Spark job for batch processing
docker exec -it news-recommender-spark-master-1 spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/data/spark_jobs/mind_processor.py

# Or run specific analytics jobs
docker exec -it news-recommender-spark-master-1 spark-submit \
  --master spark://spark-master:7077 \
  --py-files /opt/data/spark_jobs/mind_processor.py \
  --conf spark.app.name="MindBatchAnalytics" \
  /opt/data/spark_jobs/mind_processor.py
```

This will:
- Train collaborative filtering models using ALS
- Generate batch recommendations for all users
- Analyze user engagement patterns
- Calculate content popularity metrics
- Perform trend analysis

## API Endpoints

### Core Endpoints
- `GET /health` — Health check for all services
- `POST /users/` — Create a new user
- `GET /users/{user_id}` — Get user details with MIND metrics
- `GET /articles/` — List recent articles (supports MIND categories)
- `GET /search/` — Search articles using OpenSearch
- `POST /recommendations/` — Get personalized recommendations

### MIND-Specific Endpoints
- `GET /mind/stats/` — MIND dataset statistics
- `GET /mind/categories/` — Available categories from MIND
- `GET /mind/user-analytics/{user_id}` — Detailed user analytics
- `GET /mind/trending/` — Trending articles and categories
- `POST /mind/train-model/` — Trigger model retraining

### Development Endpoints
- `POST /test/populate-mind-data/` — Load sample MIND data
- `GET /admin/pipeline-status/` — Data pipeline monitoring

## Services and Architecture

### Data Flow
```
MIND Dataset → Kafka Producer → Kafka Topics → Kafka Consumer → PostgreSQL/OpenSearch
                                       ↓
                              Spark Jobs (Batch/Stream) → MLflow → Recommendations
                                       ↓
                              Real-time Metrics → Redis Cache → API Responses
```

### Service Roles
- **API (FastAPI)**: REST endpoints, business logic, real-time serving
- **Kafka**: Event streaming for news articles and user behaviors
- **PostgreSQL**: Persistent storage for users, articles, interactions
- **Redis**: Caching recommendations, user metrics, session data
- **OpenSearch**: Full-text search with category and entity filtering
- **Spark**: Large-scale data processing, model training, analytics
- **MLflow**: ML experiment tracking, model versioning, deployment

## Monitoring and Verification

### 1. Check Data Ingestion
```bash
# Verify Kafka topics
docker exec -it news-recommender-kafka-1 kafka-topics --list --bootstrap-server localhost:9092

# Check topic message counts
docker exec -it news-recommender-kafka-1 kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 --topic mind-news-articles

# Monitor consumer lag
docker exec -it news-recommender-kafka-1 kafka-consumer-groups \
  --bootstrap-server localhost:9092 --describe --all-groups
```

### 2. Verify Database Content
```bash
# Connect to PostgreSQL
docker exec -it news-recommender-postgres-1 psql -U newsuser -d newsdb

# Check data counts
SELECT 
  (SELECT COUNT(*) FROM users) AS users,
  (SELECT COUNT(*) FROM news_articles) AS articles,
  (SELECT COUNT(*) FROM user_interactions) AS interactions;
```

### 3. Monitor Spark Jobs
```bash
# Check Spark UI
open http://localhost:8080

# View running applications
docker exec -it news-recommender-spark-master-1 spark-class org.apache.spark.deploy.master.Master
```

### 4. Test Recommendations
```bash
# Get recommendations for a user
curl -X POST http://localhost:8000/recommendations/ \
  -H "Content-Type: application/json" \
  -d '{"user_id": 1, "limit": 10}'

# Search articles
curl "http://localhost:8000/search/?query=technology&limit=5"
```

## Development Workflow

### 1. Setup Development Environment
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate     # Windows

# Install dependencies
pip install -r backend/requirements.txt
pip install -r scripts/requirements_scripts.txt
```

### 2. Run Components Individually
```bash
# Start only infrastructure services
docker-compose up postgres redis opensearch mlflow kafka zookeeper spark-master spark-worker

# Run API locally
cd backend && python main.py

# Run Kafka producer manually
python scripts/kafka_producer.py

# Run Kafka consumer manually
python backend/services/kafka_consumer.py
```

### 3. Model Training and Evaluation
```bash
# Train new models
curl -X POST http://localhost:8000/mind/train-model/ \
  -H "Content-Type: application/json" \
  -d '{"algorithm": "collaborative_filtering", "retrain": true}'

# View MLflow experiments
open http://localhost:5000
```

## Troubleshooting

### Common Issues
1. **Kafka connection errors**: Ensure Kafka is fully started before running producers/consumers
2. **Spark job failures**: Check Spark worker resources and increase memory if needed
3. **Database connection**: Verify PostgreSQL is healthy and accessible
4. **MIND data not found**: Ensure dataset is extracted to correct `data/mind/` directory

### Debug Commands
```bash
# Check service logs
docker-compose logs -f api
docker-compose logs -f kafka
docker-compose logs -f spark-master

# Monitor Kafka topics
docker exec -it news-recommender-kafka-1 kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic mind-news-articles --from-beginning

# Check database connections
docker exec -it news-recommender-postgres-1 pg_isready -U newsuser

# Test OpenSearch
curl http://localhost:9200/_cluster/health
```

## Performance Optimization

### For Large MIND Dataset
- Increase Kafka partitions for parallel processing
- Tune Spark executor memory and cores
- Use Redis clustering for high-volume caching
- Implement data compression and efficient serialization

### Resource Requirements
- **Minimum**: 8GB RAM, 4 CPU cores
- **Recommended**: 16GB RAM, 8 CPU cores
- **For MINDlarge**: 32GB RAM, 16 CPU cores

## Next Steps

1. Load your MIND dataset into `./data/mind/`
2. Run `docker-compose up --build`
3. Execute `python scripts/kafka_producer.py`
4. Monitor the pipeline with `curl http://localhost:8000/health`
5. Test recommendations with the API endpoints

For detailed API documentation, visit: http://localhost:8000/docs