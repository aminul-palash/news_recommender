#!/usr/bin/env python3
"""
Kafka consumer for real-time processing of MIND dataset
"""
import os
import json
import asyncio
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import logging
from datetime import datetime
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import sys

# Add backend to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'backend'))

from database import SessionLocal, get_db, engine
from models import User, NewsArticle, UserInteraction
from services.cache_service import CacheService
from services.search_service import SearchService

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MindDataConsumer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.consumers = {}
        self.cache_service = CacheService()
        self.search_service = SearchService()
        
        # Topics to consume
        self.topics = {
            'news_articles': 'mind-news-articles',
            'user_behaviors': 'mind-user-behaviors',
            'user_profiles': 'mind-user-profiles'
        }
    
    async def initialize(self):
        """Initialize services and consumers"""
        await self.cache_service.initialize()
        await self.search_service.initialize()
        
        # Create consumers for each topic
        for topic_name, topic in self.topics.items():
            try:
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=self.bootstrap_servers,
                    group_id=f'mind-consumer-{topic_name}',
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    key_deserializer=lambda m: m.decode('utf-8') if m else None,
                    auto_offset_reset='earliest',
                    enable_auto_commit=True
                )
                self.consumers[topic_name] = consumer
                logger.info(f"Created consumer for topic: {topic}")
            except Exception as e:
                logger.error(f"Failed to create consumer for {topic}: {e}")
    
    async def process_news_articles(self):
        """Process news articles from Kafka"""
        consumer = self.consumers.get('news_articles')
        if not consumer:
            logger.error("News articles consumer not available")
            return
        
        logger.info("Starting news articles processing...")
        
        try:
            for message in consumer:
                try:
                    article_data = message.value
                    await self._store_article(article_data)
                    await self._index_article(article_data)
                    
                except Exception as e:
                    logger.error(f"Error processing article: {e}")
                    
        except KeyboardInterrupt:
            logger.info("Stopping news articles consumer...")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
    
    async def process_user_behaviors(self):
        """Process user behaviors from Kafka"""
        consumer = self.consumers.get('user_behaviors')
        if not consumer:
            logger.error("User behaviors consumer not available")
            return
        
        logger.info("Starting user behaviors processing...")
        
        try:
            for message in consumer:
                try:
                    behavior_data = message.value
                    await self._process_behavior(behavior_data)
                    await self._update_user_metrics(behavior_data)
                    
                except Exception as e:
                    logger.error(f"Error processing behavior: {e}")
                    
        except KeyboardInterrupt:
            logger.info("Stopping user behaviors consumer...")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
    
    async def process_user_profiles(self):
        """Process user profiles from Kafka"""
        consumer = self.consumers.get('user_profiles')
        if not consumer:
            logger.error("User profiles consumer not available")
            return
        
        logger.info("Starting user profiles processing...")
        
        try:
            for message in consumer:
                try:
                    profile_data = message.value
                    await self._store_user_profile(profile_data)
                    await self._cache_user_profile(profile_data)
                    
                except Exception as e:
                    logger.error(f"Error processing profile: {e}")
                    
        except KeyboardInterrupt:
            logger.info("Stopping user profiles consumer...")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
    
    async def _store_article(self, article_data: Dict):
        """Store article in PostgreSQL"""
        try:
            db = SessionLocal()
            
            # Check if article already exists
            existing = db.query(NewsArticle).filter(
                NewsArticle.url == article_data.get('news_id', article_data.get('url', ''))
            ).first()
            
            if existing:
                db.close()
                return
            
            # Create new article
            article = NewsArticle(
                title=article_data['title'],
                content=article_data['content'],
                url=article_data.get('url', article_data['news_id']),
                category=article_data['category'],
                source=article_data['source'],
                published_at=article_data.get('published_at', datetime.utcnow())
            )
            
            db.add(article)
            db.commit()
            db.refresh(article)
            db.close()
            
            logger.debug(f"Stored article: {article_data['news_id']}")
            
        except Exception as e:
            logger.error(f"Error storing article: {e}")
    
    async def _index_article(self, article_data: Dict):
        """Index article in OpenSearch"""
        try:
            # Prepare data for search indexing
            search_doc = {
                'id': article_data['news_id'],
                'title': article_data['title'],
                'content': article_data['content'],
                'category': article_data['category'],
                'subcategory': article_data.get('subcategory', ''),
                'source': article_data['source'],
                'published_at': article_data.get('published_at', datetime.utcnow()),
                'title_entities': article_data.get('title_entities', []),
                'abstract_entities': article_data.get('abstract_entities', [])
            }
            
            await self.search_service.index_article(search_doc)
            logger.debug(f"Indexed article: {article_data['news_id']}")
            
        except Exception as e:
            logger.error(f"Error indexing article: {e}")
    
    async def _process_behavior(self, behavior_data: Dict):
        """Process user behavior and store interactions"""
        try:
            db = SessionLocal()
            
            user_id = behavior_data['user_id']
            
            # Process impressions (what was shown to user)
            for impression in behavior_data['impressions']:
                interaction = UserInteraction(
                    user_id=user_id,
                    article_id=impression['news_id'],
                    interaction_type='impression',
                    timestamp=behavior_data['timestamp']
                )
                db.add(interaction)
                
                # If clicked, add click interaction
                if impression['clicked']:
                    click_interaction = UserInteraction(
                        user_id=user_id,
                        article_id=impression['news_id'],
                        interaction_type='click',
                        timestamp=behavior_data['timestamp']
                    )
                    db.add(click_interaction)
            
            # Process history (what user has clicked before)
            for news_id in behavior_data['history']:
                history_interaction = UserInteraction(
                    user_id=user_id,
                    article_id=news_id,
                    interaction_type='history_click',
                    timestamp=behavior_data['timestamp']
                )
                db.add(history_interaction)
            
            db.commit()
            db.close()
            
            logger.debug(f"Processed behavior for user: {user_id}")
            
        except Exception as e:
            logger.error(f"Error processing behavior: {e}")
    
    async def _update_user_metrics(self, behavior_data: Dict):
        """Update real-time user metrics in cache"""
        try:
            user_id = behavior_data['user_id']
            
            # Calculate metrics
            total_impressions = len(behavior_data['impressions'])
            total_clicks = sum(1 for imp in behavior_data['impressions'] if imp['clicked'])
            ctr = total_clicks / total_impressions if total_impressions > 0 else 0.0
            
            # Update cached metrics
            metrics_key = f"user_metrics:{user_id}"
            current_metrics = await self.cache_service.get(metrics_key) or {}
            
            current_metrics.update({
                'total_impressions': current_metrics.get('total_impressions', 0) + total_impressions,
                'total_clicks': current_metrics.get('total_clicks', 0) + total_clicks,
                'last_activity': behavior_data['timestamp'].isoformat(),
                'ctr': ctr
            })
            
            await self.cache_service.set(metrics_key, current_metrics, expire=86400)  # 24 hours
            
        except Exception as e:
            logger.error(f"Error updating user metrics: {e}")
    
    async def _store_user_profile(self, profile_data: Dict):
        """Store or update user profile in database"""
        try:
            db = SessionLocal()
            
            # Check if user exists
            user = db.query(User).filter(User.id == profile_data['user_id']).first()
            
            if not user:
                # Create new user with derived preferences
                user_preferences = self._derive_preferences(profile_data)
                
                user = User(
                    id=profile_data['user_id'],
                    username=f"mind_user_{profile_data['user_id']}",
                    email=f"user_{profile_data['user_id']}@mind.dataset",
                    preferences=user_preferences
                )
                db.add(user)
            else:
                # Update existing user preferences
                user.preferences = self._derive_preferences(profile_data)
            
            db.commit()
            db.close()
            
            logger.debug(f"Stored/updated user profile: {profile_data['user_id']}")
            
        except Exception as e:
            logger.error(f"Error storing user profile: {e}")
    
    def _derive_preferences(self, profile_data: Dict) -> List[str]:
        """Derive user preferences from profile data"""
        # Extract top categories from user behavior
        categories = profile_data.get('categories', {})
        
        # Sort by frequency and return top categories
        sorted_categories = sorted(categories.items(), key=lambda x: x[1], reverse=True)
        preferences = [cat for cat, _ in sorted_categories[:5]]  # Top 5 categories
        
        return preferences
    
    async def _cache_user_profile(self, profile_data: Dict):
        """Cache user profile for fast access"""
        try:
            cache_key = f"user_profile:{profile_data['user_id']}"
            await self.cache_service.set(cache_key, profile_data, expire=3600)  # 1 hour
            
        except Exception as e:
            logger.error(f"Error caching user profile: {e}")
    
    def close(self):
        """Close all consumers and services"""
        for consumer in self.consumers.values():
            consumer.close()
        
        # Close services
        asyncio.create_task(self.cache_service.close())
        asyncio.create_task(self.search_service.close())

async def run_consumers():
    """Run all consumers concurrently"""
    consumer = MindDataConsumer()
    await consumer.initialize()
    
    try:
        # Run consumers concurrently
        tasks = [
            asyncio.create_task(consumer.process_news_articles()),
            asyncio.create_task(consumer.process_user_behaviors()),
            asyncio.create_task(consumer.process_user_profiles())
        ]
        
        await asyncio.gather(*tasks)
        
    except KeyboardInterrupt:
        logger.info("Shutting down consumers...")
    finally:
        consumer.close()

if __name__ == "__main__":
    asyncio.run(run_consumers())