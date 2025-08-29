from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import os
import logging
from contextlib import asynccontextmanager
from sqlalchemy import text

from database import get_db, init_db, SessionLocal
from models import User, NewsArticle
from services.cache_service import CacheService
from services.search_service import SearchService
from services.ml_service import MLService

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize services
cache_service = CacheService()
search_service = SearchService()
ml_service = MLService()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize services on startup"""
    logger.info("Starting up News Recommender API...")
    
    # Initialize database
    init_db()
    
    # Initialize services
    await cache_service.initialize()
    await search_service.initialize()
    await ml_service.initialize()
    
    logger.info("All services initialized successfully!")
    yield
    
    # Cleanup on shutdown
    logger.info("Shutting down services...")
    await cache_service.close()
    await search_service.close()

app = FastAPI(
    title="News Recommender API",
    description="A prototype news recommendation system",
    version="0.1.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify exact origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models
class UserCreate(BaseModel):
    username: str
    email: str
    preferences: Optional[List[str]] = []

class ArticleResponse(BaseModel):
    id: int
    title: str
    content: str
    url: str
    category: str
    published_at: str
    relevance_score: Optional[float] = None

class RecommendationRequest(BaseModel):
    user_id: int
    limit: Optional[int] = 10
    categories: Optional[List[str]] = None

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint for container orchestration"""
    try:
        # Check database connection
        db = SessionLocal()
        db.execute(text("SELECT 1"))
        db.close()
        
        # Check other services
        cache_status = await cache_service.health_check()
        search_status = await search_service.health_check()
        
        return {
            "status": "healthy",
            "database": "connected",
            "cache": cache_status,
            "search": search_status,
            "version": "0.1.0"
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Service unavailable")

# User endpoints
@app.post("/users/", response_model=dict)
async def create_user(user: UserCreate, db: SessionLocal = Depends(get_db)):
    """Create a new user"""
    try:
        db_user = User(
            username=user.username,
            email=user.email,
            preferences=user.preferences
        )
        db.add(db_user)
        db.commit()
        db.refresh(db_user)
        
        return {
            "id": db_user.id,
            "username": db_user.username,
            "email": db_user.email,
            "preferences": db_user.preferences,
            "message": "User created successfully"
        }
    except Exception as e:
        logger.error(f"Error creating user: {e}")
        raise HTTPException(status_code=400, detail="Failed to create user")

@app.get("/users/{user_id}")
async def get_user(user_id: int, db: SessionLocal = Depends(get_db)):
    """Get user by ID"""
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    return {
        "id": user.id,
        "username": user.username,
        "email": user.email,
        "preferences": user.preferences
    }

# Article endpoints
@app.get("/articles/", response_model=List[ArticleResponse])
async def get_articles(
    limit: int = 10,
    category: Optional[str] = None,
    db: SessionLocal = Depends(get_db)
):
    """Get recent articles"""
    try:
        query = db.query(NewsArticle)
        
        if category:
            query = query.filter(NewsArticle.category == category)
        
        articles = query.order_by(NewsArticle.published_at.desc()).limit(limit).all()
        
        return [
            ArticleResponse(
                id=article.id,
                title=article.title,
                content=article.content[:500] + "...",  # Truncate for API response
                url=article.url,
                category=article.category,
                published_at=article.published_at.isoformat()
            )
            for article in articles
        ]
    except Exception as e:
        logger.error(f"Error fetching articles: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch articles")

# Search endpoint
@app.get("/search/")
async def search_articles(
    query: str,
    limit: int = 10,
    category: Optional[str] = None
):
    """Search articles using OpenSearch"""
    try:
        results = await search_service.search_articles(
            query=query,
            limit=limit,
            category=category
        )
        
        return {
            "query": query,
            "total_results": len(results),
            "articles": results
        }
    except Exception as e:
        logger.error(f"Search error: {e}")
        raise HTTPException(status_code=500, detail="Search failed")

# Recommendation endpoints
@app.post("/recommendations/")
async def get_recommendations(request: RecommendationRequest):
    """Get personalized recommendations for a user"""
    try:
        # Check cache first
        cache_key = f"recommendations:user:{request.user_id}:limit:{request.limit}"
        cached_recommendations = await cache_service.get(cache_key)
        
        if cached_recommendations:
            return {
                "user_id": request.user_id,
                "recommendations": cached_recommendations,
                "source": "cache"
            }
        
        # Generate recommendations using ML service
        recommendations = await ml_service.get_recommendations(
            user_id=request.user_id,
            limit=request.limit,
            categories=request.categories
        )
        
        # Cache results for 1 hour
        await cache_service.set(cache_key, recommendations, expire=3600)
        
        return {
            "user_id": request.user_id,
            "recommendations": recommendations,
            "source": "ml_model"
        }
    except Exception as e:
        logger.error(f"Recommendation error: {e}")
        raise HTTPException(status_code=500, detail="Failed to generate recommendations")

# Admin endpoints
@app.get("/admin/stats/")
async def get_system_stats(db: SessionLocal = Depends(get_db)):
    """Get system statistics"""
    try:
        user_count = db.query(User).count()
        article_count = db.query(NewsArticle).count()
        
        return {
            "users": user_count,
            "articles": article_count,
            "services": {
                "cache": await cache_service.health_check(),
                "search": await search_service.health_check(),
                "ml": await ml_service.health_check()
            }
        }
    except Exception as e:
        logger.error(f"Stats error: {e}")
        raise HTTPException(status_code=500, detail="Failed to get stats")

# Test endpoints for development
@app.post("/test/populate-sample-data/")
async def populate_sample_data(db: SessionLocal = Depends(get_db)):
    """Populate database with sample data for testing"""
    try:
        # Create sample users
        sample_users = [
            User(username="alice", email="alice@example.com", preferences=["technology", "science"]),
            User(username="bob", email="bob@example.com", preferences=["sports", "politics"]),
            User(username="charlie", email="charlie@example.com", preferences=["entertainment", "business"])
        ]
        
        for user in sample_users:
            db.add(user)
        
        # Create sample articles
        sample_articles = [
            NewsArticle(
                title="AI Breakthrough in Healthcare",
                content="Scientists have developed a new AI system that can diagnose diseases...",
                url="https://example.com/ai-healthcare",
                category="technology",
                source="TechNews"
            ),
            NewsArticle(
                title="Stock Market Reaches New Heights",
                content="The stock market closed at record highs today after...",
                url="https://example.com/stock-market",
                category="business",
                source="BusinessDaily"
            ),
            NewsArticle(
                title="Climate Change Summit Results",
                content="World leaders gathered to discuss climate action plans...",
                url="https://example.com/climate-summit",
                category="science",
                source="ScienceToday"
            )
        ]
        
        for article in sample_articles:
            db.add(article)
        
        db.commit()
        
        return {"message": "Sample data populated successfully"}
    except Exception as e:
        logger.error(f"Error populating sample data: {e}")
        raise HTTPException(status_code=500, detail="Failed to populate sample data")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)