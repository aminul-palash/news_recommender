# models.py
from sqlalchemy import Column, Integer, String, Text, DateTime, JSON, Float, Boolean
from sqlalchemy.sql import func
from database import Base

class User(Base):
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True, nullable=False)
    email = Column(String, unique=True, index=True, nullable=False)
    preferences = Column(JSON, default=list)  # List of preferred categories
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    is_active = Column(Boolean, default=True)

class NewsArticle(Base):
    __tablename__ = "news_articles"
    
    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, nullable=False, index=True)
    content = Column(Text, nullable=False)
    url = Column(String, unique=True, nullable=False)
    category = Column(String, index=True)
    source = Column(String, index=True)
    published_at = Column(DateTime(timezone=True), server_default=func.now())
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # ML features (can be populated later)
    embedding = Column(JSON)  # Vector embedding for similarity search
    popularity_score = Column(Float, default=0.0)
    quality_score = Column(Float, default=0.0)

class UserInteraction(Base):
    __tablename__ = "user_interactions"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, index=True, nullable=False)
    article_id = Column(Integer, index=True, nullable=False)
    interaction_type = Column(String, nullable=False)  # 'view', 'like', 'share', 'click'
    timestamp = Column(DateTime(timezone=True), server_default=func.now())
    duration = Column(Integer)  # Time spent reading (seconds)
    rating = Column(Float)  # Optional user rating

class Recommendation(Base):
    __tablename__ = "recommendations"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, index=True, nullable=False)
    article_id = Column(Integer, index=True, nullable=False)
    score = Column(Float, nullable=False)
    algorithm = Column(String, nullable=False)  # Which algorithm generated this
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    is_served = Column(Boolean, default=False)  # Was this recommendation shown to user?