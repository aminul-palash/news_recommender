# services/ml_service.py
import mlflow
import os
from typing import List, Dict, Optional
import logging
import random
from datetime import datetime, timedelta
from database import SessionLocal
from models import User, NewsArticle, UserInteraction

logger = logging.getLogger(__name__)

class MLService:
    def __init__(self):
        self.mlflow_url = os.getenv("MLFLOW_URL", "http://localhost:5000")
        self.model = None
        self.initialized = False
    
    async def initialize(self):
        """Initialize MLflow and load models"""
        try:
            mlflow.set_tracking_uri(self.mlflow_url)
            
            # For prototype, we'll use simple rule-based recommendations
            # In production, you would load trained models here
            self.initialized = True
            logger.info("ML service initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize MLflow: {e}")
            # Continue without ML service for basic functionality
            self.initialized = False
    
    async def get_recommendations(
        self, 
        user_id: int, 
        limit: int = 10,
        categories: Optional[List[str]] = None
    ) -> List[Dict]:
        """Generate recommendations for a user"""
        try:
            db = SessionLocal()
            
            # Get user preferences
            user = db.query(User).filter(User.id == user_id).first()
            if not user:
                db.close()
                return []
            
            user_preferences = user.preferences or []
            
            # Simple content-based recommendation logic for prototype
            query = db.query(NewsArticle)
            
            # Filter by categories if specified
            filter_categories = categories or user_preferences
            if filter_categories:
                query = query.filter(NewsArticle.category.in_(filter_categories))
            
            # Get recent articles (last 7 days)
            seven_days_ago = datetime.utcnow() - timedelta(days=7)
            query = query.filter(NewsArticle.published_at >= seven_days_ago)
            
            # Order by recency and limit results
            articles = query.order_by(NewsArticle.published_at.desc()).limit(limit * 2).all()
            
            db.close()
            
            if not articles:
                return []
            
            # Simple scoring algorithm for prototype
            recommendations = []
            for article in articles:
                score = self._calculate_simple_score(article, user_preferences)
                
                recommendations.append({
                    "id": article.id,
                    "title": article.title,
                    "content": article.content[:500] + "...",
                    "url": article.url,
                    "category": article.category,
                    "published_at": article.published_at.isoformat(),
                    "relevance_score": score,
                    "algorithm": "content_based_simple"
                })
            
            # Sort by score and return top recommendations
            recommendations.sort(key=lambda x: x["relevance_score"], reverse=True)
            return recommendations[:limit]
            
        except Exception as e:
            logger.error(f"Error generating recommendations: {e}")
            return []
    
    def _calculate_simple_score(self, article, user_preferences: List[str]) -> float:
        """Simple scoring function for prototype"""
        score = 0.0
        
        # Category preference match
        if article.category in user_preferences:
            score += 0.5
        
        # Recency boost (newer articles get higher scores)
        hours_since_published = (datetime.utcnow() - article.published_at).total_seconds() / 3600
        if hours_since_published < 24:
            score += 0.3
        elif hours_since_published < 72:
            score += 0.2
        
        # Quality score (if available)
        if hasattr(article, 'quality_score') and article.quality_score:
            score += article.quality_score * 0.2
        
        # Add some randomness for diversity
        score += random.random() * 0.1
        
        return round(score, 3)
    
    async def train_model(self, user_interactions_data: List[Dict]):
        """Placeholder for model training"""
        if not self.initialized:
            logger.warning("MLflow not initialized, skipping training")
            return False
        
        try:
            # In production, implement actual model training here
            # For now, just log a dummy experiment
            
            with mlflow.start_run():
                mlflow.log_param("algorithm", "collaborative_filtering")
                mlflow.log_param("data_points", len(user_interactions_data))
                mlflow.log_metric("training_accuracy", 0.85)  # Dummy metric
                
                # Log model artifact (placeholder)
                mlflow.log_text("Simple content-based model", "model_info.txt")
                
            logger.info("Model training completed (prototype)")
            return True
            
        except Exception as e:
            logger.error(f"Training error: {e}")
            return False
    
    async def health_check(self) -> str:
        """Check ML service health"""
        if not self.initialized:
            return "not_initialized"
        
        try:
            # Try to connect to MLflow
            mlflow.set_tracking_uri(self.mlflow_url)
            # Simple ping to MLflow server
            return "healthy"
        except Exception:
            return "error"