# services/search_service.py
from opensearchpy import AsyncOpenSearch
import os
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)

class SearchService:
    def __init__(self):
        self.opensearch_url = os.getenv("OPENSEARCH_URL", "http://localhost:9200")
        self.client = None
        self.index_name = "news_articles"
    
    async def initialize(self):
        """Initialize OpenSearch connection and create index"""
        try:
            self.client = AsyncOpenSearch([self.opensearch_url])
            
            # Check if client is connected
            info = await self.client.info()
            logger.info(f"OpenSearch connected: {info['version']['number']}")
            
            # Create index if it doesn't exist
            await self._create_index_if_not_exists()
            
            logger.info("Search service initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize OpenSearch: {e}")
            # For prototype, continue without search
            self.client = None
    
    async def _create_index_if_not_exists(self):
        """Create the news articles index with proper mapping"""
        if not self.client:
            return
        
        try:
            index_exists = await self.client.indices.exists(index=self.index_name)
            
            if not index_exists:
                # Define index mapping
                mapping = {
                    "mappings": {
                        "properties": {
                            "id": {"type": "integer"},
                            "title": {"type": "text", "analyzer": "standard"},
                            "content": {"type": "text", "analyzer": "standard"},
                            "category": {"type": "keyword"},
                            "source": {"type": "keyword"},
                            "url": {"type": "keyword"},
                            "published_at": {"type": "date"},
                            "popularity_score": {"type": "float"},
                            "quality_score": {"type": "float"}
                        }
                    },
                    "settings": {
                        "number_of_shards": 1,
                        "number_of_replicas": 0
                    }
                }
                
                await self.client.indices.create(
                    index=self.index_name,
                    body=mapping
                )
                logger.info(f"Created OpenSearch index: {self.index_name}")
        except Exception as e:
            logger.error(f"Error creating index: {e}")
    
    async def index_article(self, article_data: Dict):
        """Index a single article"""
        if not self.client:
            return False
        
        try:
            await self.client.index(
                index=self.index_name,
                id=article_data["id"],
                body=article_data
            )
            return True
        except Exception as e:
            logger.error(f"Error indexing article {article_data.get('id')}: {e}")
            return False
    
    async def search_articles(
        self, 
        query: str, 
        limit: int = 10,
        category: Optional[str] = None
    ) -> List[Dict]:
        """Search articles using OpenSearch"""
        if not self.client:
            # Fallback to empty results if search is unavailable
            return []
        
        try:
            # Build search query
            search_body = {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "multi_match": {
                                    "query": query,
                                    "fields": ["title^2", "content"],
                                    "type": "best_fields"
                                }
                            }
                        ]
                    }
                },
                "size": limit,
                "_source": ["id", "title", "content", "url", "category", "published_at"],
                "sort": [
                    {"_score": {"order": "desc"}},
                    {"published_at": {"order": "desc"}}
                ]
            }
            
            # Add category filter if specified
            if category:
                search_body["query"]["bool"]["filter"] = [
                    {"term": {"category": category}}
                ]
            
            response = await self.client.search(
                index=self.index_name,
                body=search_body
            )
            
            # Extract results
            results = []
            for hit in response["hits"]["hits"]:
                source = hit["_source"]
                source["relevance_score"] = hit["_score"]
                # Truncate content for API response
                if len(source["content"]) > 500:
                    source["content"] = source["content"][:500] + "..."
                results.append(source)
            
            return results
            
        except Exception as e:
            logger.error(f"Search error: {e}")
            return []
    
    async def health_check(self) -> str:
        """Check search service health"""
        if not self.client:
            return "disconnected"
        
        try:
            info = await self.client.info()
            return "healthy"
        except Exception:
            return "error"
    
    async def close(self):
        """Close OpenSearch connection"""
        if self.client:
            await self.client.close()