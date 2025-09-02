# models.py - Updated for MIND dataset
from sqlalchemy import Column, Integer, String, Text, DateTime, JSON, Float, Boolean, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from database import Base

class User(Base):
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, index=True)
    mind_user_id = Column(String, unique=True, index=True)  # Original MIND user ID
    username = Column(String, unique=True, index=True, nullable=False)
    email = Column(String, unique=True, index=True, nullable=False)
    preferences = Column(JSON, default=list)  # List of preferred categories
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    is_active = Column(Boolean, default=True)
    
    # MIND-specific fields
    total_impressions = Column(Integer, default=0)
    total_clicks = Column(Integer, default=0)
    click_through_rate = Column(Float, default=0.0)
    engagement_score = Column(Float, default=0.0)
    engagement_category = Column(String, default='low')  # low, medium, high
    first_activity = Column(DateTime(timezone=True))
    last_activity = Column(DateTime(timezone=True))
    
    # Relationships
    interactions = relationship("UserInteraction", back_populates="user")

class NewsArticle(Base):
    __tablename__ = "news_articles"
    
    id = Column(Integer, primary_key=True, index=True)
    news_id = Column(String, unique=True, index=True, nullable=False)  # MIND news ID
    title = Column(String, nullable=False, index=True)
    content = Column(Text, nullable=False)  # Abstract from MIND
    url = Column(String, unique=True, nullable=False)
    category = Column(String, index=True)
    subcategory = Column(String, index=True)
    source = Column(String, index=True, default='MIND')
    published_at = Column(DateTime(timezone=True), server_default=func.now())
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # MIND-specific fields
    title_entities = Column(JSON)  # Named entities in title
    abstract_entities = Column(JSON)  # Named entities in abstract
    
    # ML features
    embedding = Column(JSON)  # Vector embedding for similarity search
    popularity_score = Column(Float, default=0.0)
    quality_score = Column(Float, default=0.0)
    
    # Calculated metrics from interactions
    total_impressions = Column(Integer, default=0)
    total_clicks = Column(Integer, default=0)
    unique_users = Column(Integer, default=0)
    click_through_rate = Column(Float, default=0.0)
    
    # Relationships
    interactions = relationship("UserInteraction", back_populates="article")

class UserInteraction(Base):
    __tablename__ = "user_interactions"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True, nullable=False)
    article_id = Column(Integer, ForeignKey("news_articles.id"), index=True, nullable=False)
    
    # MIND-specific fields
    impression_id = Column(String, index=True)  # MIND impression ID
    news_id = Column(String, index=True)  # MIND news ID
    mind_user_id = Column(String, index=True)  # MIND user ID
    
    interaction_type = Column(String, nullable=False)  # 'impression', 'click', 'history_click'
    timestamp = Column(DateTime(timezone=True), server_default=func.now())
    duration = Column(Integer)  # Time spent reading (seconds)
    rating = Column(Float)  # Implicit rating based on interaction
    
    # Context information
    position_in_impression = Column(Integer)  # Position of article in impression list
    total_impressions_in_session = Column(Integer)  # Total articles shown in session
    
    # Relationships
    user = relationship("User", back_populates="interactions")
    article = relationship("NewsArticle", back_populates="interactions")

class Recommendation(Base):
    __tablename__ = "recommendations"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True, nullable=False)
    article_id = Column(Integer, ForeignKey("news_articles.id"), index=True, nullable=False)
    
    score = Column(Float, nullable=False)
    algorithm = Column(String, nullable=False)  # 'collaborative_filtering', 'content_based', 'hybrid'
    model_version = Column(String, default='v1.0')
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    is_served = Column(Boolean, default=False)  # Was this recommendation shown to user?
    served_at = Column(DateTime(timezone=True))
    
    # Performance tracking
    was_clicked = Column(Boolean, default=False)
    click_timestamp = Column(DateTime(timezone=True))
    
    # Context
    recommendation_context = Column(JSON)  # Additional context for recommendation

class UserSession(Base):
    __tablename__ = "user_sessions"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True, nullable=False)
    session_id = Column(String, unique=True, index=True, nullable=False)
    
    start_time = Column(DateTime(timezone=True), server_default=func.now())
    end_time = Column(DateTime(timezone=True))
    duration_seconds = Column(Integer)
    
    # Session metrics
    articles_viewed = Column(Integer, default=0)
    articles_clicked = Column(Integer, default=0)
    categories_explored = Column(JSON)  # List of categories viewed
    
    # Behavior patterns
    avg_time_per_article = Column(Float)
    bounce_rate = Column(Float)  # Did user leave quickly?
    engagement_depth = Column(Float)  # How deep did user engage?

class ContentMetrics(Base):
    __tablename__ = "content_metrics"
    
    id = Column(Integer, primary_key=True, index=True)
    article_id = Column(Integer, ForeignKey("news_articles.id"), unique=True, nullable=False)
    
    # Real-time metrics
    views_last_hour = Column(Integer, default=0)
    views_last_day = Column(Integer, default=0)
    clicks_last_hour = Column(Integer, default=0)
    clicks_last_day = Column(Integer, default=0)
    
    # Trending metrics
    trending_score = Column(Float, default=0.0)
    velocity = Column(Float, default=0.0)  # Rate of engagement change
    
    # Quality metrics
    avg_reading_time = Column(Float)
    completion_rate = Column(Float)  # How many users read to the end
    share_rate = Column(Float)
    
    last_updated = Column(DateTime(timezone=True), server_default=func.now())

class MLModelMetrics(Base):
    __tablename__ = "ml_model_metrics"
    
    id = Column(Integer, primary_key=True, index=True)
    model_name = Column(String, nullable=False)
    model_version = Column(String, nullable=False)
    algorithm = Column(String, nullable=False)
    
    # Training metrics
    training_rmse = Column(Float)
    training_accuracy = Column(Float)
    training_precision = Column(Float)
    training_recall = Column(Float)
    training_f1 = Column(Float)
    
    # Validation metrics
    validation_rmse = Column(Float)
    validation_accuracy = Column(Float)
    validation_precision = Column(Float)
    validation_recall = Column(Float)
    validation_f1 = Column(Float)
    
    # Production metrics
    online_ctr = Column(Float)  # Click-through rate in production
    online_conversion_rate = Column(Float)
    user_satisfaction = Column(Float)
    
    # Model info
    training_data_size = Column(Integer)
    feature_count = Column(Integer)
    hyperparameters = Column(JSON)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    deployed_at = Column(DateTime(timezone=True))
    
class CategoryTrends(Base):
    __tablename__ = "category_trends"
    
    id = Column(Integer, primary_key=True, index=True)
    category = Column(String, nullable=False, index=True)
    subcategory = Column(String, index=True)
    
    # Time-based metrics
    date = Column(DateTime(timezone=True), nullable=False, index=True)
    hour = Column(Integer, index=True)  # Hour of day (0-23)
    
    # Engagement metrics
    total_articles = Column(Integer, default=0)
    total_impressions = Column(Integer, default=0)
    total_clicks = Column(Integer, default=0)
    unique_users = Column(Integer, default=0)
    
    # Derived metrics
    avg_ctr = Column(Float, default=0.0)
    popularity_rank = Column(Integer)
    trend_direction = Column(String)  # 'up', 'down', 'stable'
    trend_strength = Column(Float, default=0.0)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class DataPipelineStatus(Base):
    __tablename__ = "data_pipeline_status"
    
    id = Column(Integer, primary_key=True, index=True)
    pipeline_name = Column(String, nullable=False, index=True)
    component = Column(String, nullable=False)  # 'kafka_producer', 'kafka_consumer', 'spark_job'
    
    status = Column(String, nullable=False)  # 'running', 'stopped', 'failed', 'completed'
    start_time = Column(DateTime(timezone=True))
    end_time = Column(DateTime(timezone=True))
    
    # Processing metrics
    records_processed = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)
    processing_rate = Column(Float)  # Records per second
    
    # Error information
    error_message = Column(Text)
    error_details = Column(JSON)
    
    # Resource usage
    memory_usage_mb = Column(Float)
    cpu_usage_percent = Column(Float)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())