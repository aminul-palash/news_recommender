#!/usr/bin/env python3
"""
Kafka producer for ingesting Microsoft MIND dataset
"""
import os
import json
import asyncio
import pandas as pd
from kafka import KafkaProducer
from typing import Dict, List
import logging
from datetime import datetime
import hashlib

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MindDatasetProducer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        
        # Topic names for different data types
        self.topics = {
            'news_articles': 'mind-news-articles',
            'user_behaviors': 'mind-user-behaviors',
            'user_profiles': 'mind-user-profiles'
        }
    
    def connect(self):
        """Initialize Kafka producer"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8') if k else None,
                acks='all',
                retries=3,
                retry_backoff_ms=1000
            )
            logger.info("Kafka producer connected successfully")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
    
    def parse_mind_news(self, news_file_path: str) -> List[Dict]:
        """Parse MIND news.tsv file"""
        try:
            # MIND news.tsv format: NewsID, Category, SubCategory, Title, Abstract, URL, TitleEntities, AbstractEntities
            df = pd.read_csv(
                news_file_path, 
                sep='\t', 
                header=None,
                names=['news_id', 'category', 'subcategory', 'title', 'abstract', 'url', 'title_entities', 'abstract_entities']
            )
            
            articles = []
            for _, row in df.iterrows():
                article = {
                    'news_id': row['news_id'],
                    'title': row['title'],
                    'content': row['abstract'] if pd.notna(row['abstract']) else '',
                    'category': row['category'],
                    'subcategory': row['subcategory'] if pd.notna(row['subcategory']) else '',
                    'url': row['url'] if pd.notna(row['url']) else '',
                    'title_entities': self._parse_entities(row['title_entities']),
                    'abstract_entities': self._parse_entities(row['abstract_entities']),
                    'source': 'MIND_dataset',
                    'published_at': datetime.utcnow(),
                    'ingestion_timestamp': datetime.utcnow()
                }
                articles.append(article)
            
            logger.info(f"Parsed {len(articles)} news articles")
            return articles
            
        except Exception as e:
            logger.error(f"Error parsing news file: {e}")
            return []
    
    def parse_mind_behaviors(self, behaviors_file_path: str) -> List[Dict]:
        """Parse MIND behaviors.tsv file"""
        try:
            # MIND behaviors.tsv format: ImpressionID, UserID, Time, History, Impressions
            df = pd.read_csv(
                behaviors_file_path,
                sep='\t',
                header=None,
                names=['impression_id', 'user_id', 'time', 'history', 'impressions']
            )
            
            behaviors = []
            for _, row in df.iterrows():
                # Parse history (clicked articles)
                history = self._parse_history(row['history'])
                
                # Parse impressions (shown articles with click labels)
                impressions = self._parse_impressions(row['impressions'])
                
                behavior = {
                    'impression_id': row['impression_id'],
                    'user_id': row['user_id'],
                    'timestamp': pd.to_datetime(row['time']),
                    'history': history,
                    'impressions': impressions,
                    'ingestion_timestamp': datetime.utcnow()
                }
                behaviors.append(behavior)
            
            logger.info(f"Parsed {len(behaviors)} user behaviors")
            return behaviors
            
        except Exception as e:
            logger.error(f"Error parsing behaviors file: {e}")
            return []
    
    def _parse_entities(self, entities_str) -> List[Dict]:
        """Parse entity JSON string from MIND dataset"""
        if pd.isna(entities_str) or not entities_str:
            return []
        
        try:
            entities = json.loads(entities_str)
            return entities
        except json.JSONDecodeError:
            return []
    
    def _parse_history(self, history_str) -> List[str]:
        """Parse user click history"""
        if pd.isna(history_str) or not history_str:
            return []
        
        return history_str.split()
    
    def _parse_impressions(self, impressions_str) -> List[Dict]:
        """Parse impressions with click labels"""
        if pd.isna(impressions_str) or not impressions_str:
            return []
        
        impressions = []
        for impression in impressions_str.split():
            parts = impression.split('-')
            if len(parts) == 2:
                news_id, clicked = parts
                impressions.append({
                    'news_id': news_id,
                    'clicked': clicked == '1'
                })
        
        return impressions
    
    def send_articles(self, articles: List[Dict]):
        """Send articles to Kafka topic"""
        if not self.producer:
            logger.error("Producer not connected")
            return
        
        topic = self.topics['news_articles']
        sent_count = 0
        
        for article in articles:
            try:
                # Use news_id as message key for partitioning
                key = article['news_id']
                
                self.producer.send(
                    topic=topic,
                    key=key,
                    value=article
                )
                sent_count += 1
                
                if sent_count % 1000 == 0:
                    logger.info(f"Sent {sent_count} articles to Kafka")
                    
            except Exception as e:
                logger.error(f"Error sending article {article.get('news_id')}: {e}")
        
        self.producer.flush()
        logger.info(f"Successfully sent {sent_count} articles to topic {topic}")
    
    def send_behaviors(self, behaviors: List[Dict]):
        """Send user behaviors to Kafka topic"""
        if not self.producer:
            logger.error("Producer not connected")
            return
        
        topic = self.topics['user_behaviors']
        sent_count = 0
        
        for behavior in behaviors:
            try:
                # Use user_id as message key for partitioning
                key = behavior['user_id']
                
                self.producer.send(
                    topic=topic,
                    key=key,
                    value=behavior
                )
                sent_count += 1
                
                if sent_count % 1000 == 0:
                    logger.info(f"Sent {sent_count} behaviors to Kafka")
                    
            except Exception as e:
                logger.error(f"Error sending behavior {behavior.get('impression_id')}: {e}")
        
        self.producer.flush()
        logger.info(f"Successfully sent {sent_count} behaviors to topic {topic}")
    
    def create_user_profiles(self, behaviors: List[Dict]) -> List[Dict]:
        """Create user profiles from behaviors data"""
        user_profiles = {}
        
        for behavior in behaviors:
            user_id = behavior['user_id']
            
            if user_id not in user_profiles:
                user_profiles[user_id] = {
                    'user_id': user_id,
                    'total_impressions': 0,
                    'total_clicks': 0,
                    'categories': {},
                    'first_seen': behavior['timestamp'],
                    'last_seen': behavior['timestamp']
                }
            
            profile = user_profiles[user_id]
            
            # Update timestamps
            if behavior['timestamp'] < profile['first_seen']:
                profile['first_seen'] = behavior['timestamp']
            if behavior['timestamp'] > profile['last_seen']:
                profile['last_seen'] = behavior['timestamp']
            
            # Count impressions and clicks
            for impression in behavior['impressions']:
                profile['total_impressions'] += 1
                if impression['clicked']:
                    profile['total_clicks'] += 1
        
        # Convert to list and add derived metrics
        profiles_list = []
        for profile in user_profiles.values():
            profile['click_through_rate'] = (
                profile['total_clicks'] / profile['total_impressions'] 
                if profile['total_impressions'] > 0 else 0.0
            )
            profile['ingestion_timestamp'] = datetime.utcnow()
            profiles_list.append(profile)
        
        return profiles_list
    
    def send_user_profiles(self, profiles: List[Dict]):
        """Send user profiles to Kafka topic"""
        if not self.producer:
            logger.error("Producer not connected")
            return
        
        topic = self.topics['user_profiles']
        sent_count = 0
        
        for profile in profiles:
            try:
                key = profile['user_id']
                
                self.producer.send(
                    topic=topic,
                    key=key,
                    value=profile
                )
                sent_count += 1
                
            except Exception as e:
                logger.error(f"Error sending profile {profile.get('user_id')}: {e}")
        
        self.producer.flush()
        logger.info(f"Successfully sent {sent_count} user profiles to topic {topic}")
    
    def ingest_mind_dataset(self, data_dir: str):
        """Ingest complete MIND dataset"""
        logger.info("Starting MIND dataset ingestion...")
        
        # File paths
        news_file = os.path.join(data_dir, 'news.tsv')
        behaviors_file = os.path.join(data_dir, 'behaviors.tsv')
        
        # Check if files exist
        if not os.path.exists(news_file):
            logger.error(f"News file not found: {news_file}")
            return
        
        if not os.path.exists(behaviors_file):
            logger.error(f"Behaviors file not found: {behaviors_file}")
            return
        
        # Process and send articles
        logger.info("Processing news articles...")
        articles = self.parse_mind_news(news_file)
        if articles:
            self.send_articles(articles)
        
        # Process and send behaviors
        logger.info("Processing user behaviors...")
        behaviors = self.parse_mind_behaviors(behaviors_file)
        if behaviors:
            self.send_behaviors(behaviors)
            
            # Generate and send user profiles
            logger.info("Generating user profiles...")
            profiles = self.create_user_profiles(behaviors)
            self.send_user_profiles(profiles)
        
        logger.info("MIND dataset ingestion completed!")
    
    def close(self):
        """Close producer connection"""
        if self.producer:
            self.producer.close()

def main():
    # Configuration
    data_dir = os.getenv('MIND_DATA_DIR', './data/mind')
    kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    
    producer = MindDatasetProducer(bootstrap_servers=kafka_servers)
    
    try:
        producer.connect()
        producer.ingest_mind_dataset(data_dir)
    finally:
        producer.close()

if __name__ == "__main__":
    main()