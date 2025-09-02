#!/usr/bin/env python3
"""
Spark jobs for processing MIND dataset and generating recommendations
"""
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MindSparkProcessor:
    def __init__(self, app_name="MindDataProcessor"):
        self.spark = None
        self.app_name = app_name
    
    def initialize_spark(self):
        """Initialize Spark session with optimized configuration"""
        try:
            self.spark = SparkSession.builder \
                .appName(self.app_name) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                .getOrCreate()
            
            self.spark.sparkContext.setLogLevel("WARN")
            logger.info("Spark session initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Spark: {e}")
            raise
    
    def read_kafka_stream(self, topic: str, bootstrap_servers='kafka:9092'):
        """Read streaming data from Kafka topic"""
        try:
            df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", bootstrap_servers) \
                .option("subscribe", topic) \
                .option("startingOffsets", "latest") \
                .load()
            
            return df
            
        except Exception as e:
            logger.error(f"Error reading from Kafka topic {topic}: {e}")
            return None
    
    def process_news_articles_stream(self):
        """Process streaming news articles"""
        try:
            # Read from Kafka
            kafka_df = self.read_kafka_stream('mind-news-articles')
            
            if kafka_df is None:
                return
            
            # Parse JSON from Kafka value
            articles_df = kafka_df.select(
                from_json(col("value").cast("string"), self._get_article_schema()).alias("article")
            ).select("article.*")
            
            # Add processing timestamp
            processed_df = articles_df.withColumn("processing_timestamp", current_timestamp())
            
            # Write to console for monitoring (in production, write to database/storage)
            query = processed_df.writeStream \
                .outputMode("append") \
                .format("console") \
                .option("truncate", "false") \
                .trigger(processingTime='10 seconds') \
                .start()
            
            return query
            
        except Exception as e:
            logger.error(f"Error processing news articles stream: {e}")
            return None
    
    def process_user_behaviors_stream(self):
        """Process streaming user behaviors"""
        try:
            # Read from Kafka
            kafka_df = self.read_kafka_stream('mind-user-behaviors')
            
            if kafka_df is None:
                return
            
            # Parse JSON from Kafka value
            behaviors_df = kafka_df.select(
                from_json(col("value").cast("string"), self._get_behavior_schema()).alias("behavior")
            ).select("behavior.*")
            
            # Calculate real-time metrics
            metrics_df = behaviors_df \
                .withColumn("click_count", size(filter(col("impressions"), lambda x: x.clicked == True))) \
                .withColumn("impression_count", size(col("impressions"))) \
                .withColumn("ctr", col("click_count") / col("impression_count")) \
                .withColumn("processing_timestamp", current_timestamp())
            
            # Write aggregated metrics
            query = metrics_df \
                .writeStream \
                .outputMode("append") \
                .format("console") \
                .option("truncate", "false") \
                .trigger(processingTime='30 seconds') \
                .start()
            
            return query
            
        except Exception as e:
            logger.error(f"Error processing user behaviors stream: {e}")
            return None
    
    def train_collaborative_filtering_model(self, interactions_path: str, output_path: str):
        """Train collaborative filtering model using ALS"""
        try:
            logger.info("Starting collaborative filtering model training...")
            
            # Read interactions data
            interactions_df = self.spark.read.parquet(interactions_path)
            
            # Prepare data for ALS
            # Convert string IDs to numeric for ALS
            user_indexer = StringIndexer(inputCol="user_id", outputCol="user_index")
            article_indexer = StringIndexer(inputCol="article_id", outputCol="article_index")
            
            indexed_df = user_indexer.fit(interactions_df).transform(interactions_df)
            indexed_df = article_indexer.fit(indexed_df).transform(indexed_df)
            
            # Create implicit ratings (1 for click, 0.5 for impression)
            ratings_df = indexed_df.withColumn(
                "rating",
                when(col("interaction_type") == "click", 1.0)
                .when(col("interaction_type") == "impression", 0.5)
                .otherwise(0.1)
            ).select("user_index", "article_index", "rating")
            
            # Split data
            train_df, test_df = ratings_df.randomSplit([0.8, 0.2], seed=42)
            
            # Configure ALS
            als = ALS(
                maxIter=10,
                regParam=0.1,
                userCol="user_index",
                itemCol="article_index",
                ratingCol="rating",
                coldStartStrategy="drop",
                implicitPrefs=True
            )
            
            # Train model
            model = als.fit(train_df)
            
            # Evaluate model
            predictions = model.transform(test_df)
            evaluator = RegressionEvaluator(
                metricName="rmse",
                labelCol="rating",
                predictionCol="prediction"
            )
            rmse = evaluator.evaluate(predictions)
            logger.info(f"Model RMSE: {rmse}")
            
            # Save model
            model.write().overwrite().save(output_path)
            logger.info(f"Model saved to: {output_path}")
            
            return model
            
        except Exception as e:
            logger.error(f"Error training collaborative filtering model: {e}")
            return None
    
    def generate_batch_recommendations(self, model_path: str, users_path: str, output_path: str):
        """Generate batch recommendations for all users"""
        try:
            logger.info("Generating batch recommendations...")
            
            # Load trained model
            from pyspark.ml.recommendation import ALSModel
            model = ALSModel.load(model_path)
            
            # Read users data
            users_df = self.spark.read.parquet(users_path)
            
            # Generate recommendations for all users
            user_recs = model.recommendForAllUsers(20)  # Top 20 recommendations per user
            
            # Add timestamp and algorithm info
            recommendations_df = user_recs \
                .withColumn("generated_at", current_timestamp()) \
                .withColumn("algorithm", lit("collaborative_filtering")) \
                .withColumn("model_version", lit("v1.0"))
            
            # Save recommendations
            recommendations_df.write \
                .mode("overwrite") \
                .parquet(output_path)
            
            logger.info(f"Batch recommendations saved to: {output_path}")
            
        except Exception as e:
            logger.error(f"Error generating batch recommendations: {e}")
    
    def analyze_user_engagement(self, behaviors_path: str, output_path: str):
        """Analyze user engagement patterns"""
        try:
            logger.info("Analyzing user engagement patterns...")
            
            # Read behaviors data
            behaviors_df = self.spark.read.parquet(behaviors_path)
            
            # Calculate engagement metrics per user
            engagement_df = behaviors_df \
                .groupBy("user_id") \
                .agg(
                    count("impression_id").alias("total_sessions"),
                    sum(size("impressions")).alias("total_impressions"),
                    sum(expr("size(filter(impressions, x -> x.clicked))")).alias("total_clicks"),
                    avg(expr("size(filter(impressions, x -> x.clicked)) / size(impressions)")).alias("avg_ctr"),
                    countDistinct("timestamp").alias("active_days"),
                    min("timestamp").alias("first_seen"),
                    max("timestamp").alias("last_seen")
                ) \
                .withColumn("engagement_score", 
                    col("avg_ctr") * 0.4 + 
                    (col("total_clicks") / 100.0).cast("double") * 0.3 + 
                    (col("active_days") / 30.0).cast("double") * 0.3
                )
            
            # Categorize users by engagement level
            engagement_with_category = engagement_df \
                .withColumn("engagement_category",
                    when(col("engagement_score") >= 0.7, "high")
                    .when(col("engagement_score") >= 0.4, "medium")
                    .otherwise("low")
                )
            
            # Save analysis results
            engagement_with_category.write \
                .mode("overwrite") \
                .parquet(output_path)
            
            # Show summary statistics
            engagement_with_category.groupBy("engagement_category").count().show()
            
            logger.info(f"User engagement analysis saved to: {output_path}")
            
        except Exception as e:
            logger.error(f"Error analyzing user engagement: {e}")
    
    def analyze_content_popularity(self, articles_path: str, interactions_path: str, output_path: str):
        """Analyze content popularity and trends"""
        try:
            logger.info("Analyzing content popularity...")
            
            # Read data
            articles_df = self.spark.read.parquet(articles_path)
            interactions_df = self.spark.read.parquet(interactions_path)
            
            # Calculate article metrics
            article_metrics = interactions_df \
                .groupBy("article_id") \
                .agg(
                    count("*").alias("total_interactions"),
                    sum(when(col("interaction_type") == "click", 1).otherwise(0)).alias("total_clicks"),
                    sum(when(col("interaction_type") == "impression", 1).otherwise(0)).alias("total_impressions"),
                    countDistinct("user_id").alias("unique_users")
                ) \
                .withColumn("ctr", col("total_clicks") / col("total_impressions")) \
                .withColumn("popularity_score", 
                    col("ctr") * 0.5 + 
                    (col("unique_users") / 1000.0).cast("double") * 0.3 + 
                    (col("total_clicks") / 100.0).cast("double") * 0.2
                )
            
            # Join with article details
            content_analysis = articles_df.join(article_metrics, "article_id", "left") \
                .fillna(0, ["total_interactions", "total_clicks", "total_impressions", "unique_users", "ctr", "popularity_score"])
            
            # Category-wise analysis
            category_stats = content_analysis \
                .groupBy("category") \
                .agg(
                    count("*").alias("total_articles"),
                    avg("popularity_score").alias("avg_popularity"),
                    avg("ctr").alias("avg_ctr"),
                    sum("total_clicks").alias("category_total_clicks")
                ) \
                .orderBy(desc("avg_popularity"))
            
            # Save results
            content_analysis.write.mode("overwrite").parquet(f"{output_path}/content_analysis")
            category_stats.write.mode("overwrite").parquet(f"{output_path}/category_stats")
            
            # Show top performing content
            logger.info("Top 10 most popular articles:")
            content_analysis.orderBy(desc("popularity_score")).select(
                "article_id", "title", "category", "popularity_score", "ctr"
            ).show(10, truncate=False)
            
            logger.info("Category performance:")
            category_stats.show()
            
        except Exception as e:
            logger.error(f"Error analyzing content popularity: {e}")
    
    def _get_article_schema(self):
        """Define schema for news articles"""
        return StructType([
            StructField("news_id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("content", StringType(), True),
            StructField("category", StringType(), True),
            StructField("subcategory", StringType(), True),
            StructField("url", StringType(), True),
            StructField("source", StringType(), True),
            StructField("published_at", TimestampType(), True),
            StructField("title_entities", ArrayType(MapType(StringType(), StringType())), True),
            StructField("abstract_entities", ArrayType(MapType(StringType(), StringType())), True)
        ])
    
    def _get_behavior_schema(self):
        """Define schema for user behaviors"""
        impression_schema = StructType([
            StructField("news_id", StringType(), True),
            StructField("clicked", BooleanType(), True)
        ])
        
        return StructType([
            StructField("impression_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("history", ArrayType(StringType()), True),
            StructField("impressions", ArrayType(impression_schema), True)
        ])
    
    def process_mind_dataset_batch(self, data_dir: str, output_dir: str):
        """Process entire MIND dataset in batch mode"""
        try:
            logger.info("Starting batch processing of MIND dataset...")
            
            # Read and process news articles
            news_path = os.path.join(data_dir, "news.tsv")
            if os.path.exists(news_path):
                news_df = self._read_mind_news(news_path)
                news_df.write.mode("overwrite").parquet(f"{output_dir}/articles")
                logger.info("News articles processed and saved")
            
            # Read and process behaviors
            behaviors_path = os.path.join(data_dir, "behaviors.tsv")
            if os.path.exists(behaviors_path):
                behaviors_df = self._read_mind_behaviors(behaviors_path)
                behaviors_df.write.mode("overwrite").parquet(f"{output_dir}/behaviors")
                
                # Create interactions table
                interactions_df = self._create_interactions_from_behaviors(behaviors_df)
                interactions_df.write.mode("overwrite").parquet(f"{output_dir}/interactions")
                
                logger.info("User behaviors and interactions processed and saved")
            
            # Generate user profiles
            if os.path.exists(f"{output_dir}/interactions"):
                profiles_df = self._generate_user_profiles(f"{output_dir}/interactions")
                profiles_df.write.mode("overwrite").parquet(f"{output_dir}/user_profiles")
                logger.info("User profiles generated and saved")
            
        except Exception as e:
            logger.error(f"Error in batch processing: {e}")
    
    def _read_mind_news(self, file_path: str):
        """Read MIND news.tsv file"""
        schema = StructType([
            StructField("news_id", StringType(), True),
            StructField("category", StringType(), True),
            StructField("subcategory", StringType(), True),
            StructField("title", StringType(), True),
            StructField("abstract", StringType(), True),
            StructField("url", StringType(), True),
            StructField("title_entities", StringType(), True),
            StructField("abstract_entities", StringType(), True)
        ])
        
        df = self.spark.read.option("delimiter", "\t").csv(file_path, schema=schema)
        
        # Clean and transform data
        cleaned_df = df \
            .withColumn("content", coalesce(col("abstract"), lit(""))) \
            .withColumn("source", lit("MIND_dataset")) \
            .withColumn("published_at", current_timestamp()) \
            .select("news_id", "title", "content", "category", "subcategory", "url", "source", "published_at")
        
        return cleaned_df
    
    def _read_mind_behaviors(self, file_path: str):
        """Read MIND behaviors.tsv file"""
        schema = StructType([
            StructField("impression_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("time", StringType(), True),
            StructField("history", StringType(), True),
            StructField("impressions", StringType(), True)
        ])
        
        df = self.spark.read.option("delimiter", "\t").csv(file_path, schema=schema)
        
        # Parse timestamp
        behaviors_df = df.withColumn("timestamp", 
            to_timestamp(col("time"), "M/d/yyyy h:m:s a")
        )
        
        return behaviors_df
    
    def _create_interactions_from_behaviors(self, behaviors_df):
        """Convert behaviors to individual interaction records"""
        # Explode impressions to create individual interaction records
        impressions_df = behaviors_df \
            .select("user_id", "timestamp", explode(split("impressions", " ")).alias("impression")) \
            .filter(col("impression") != "") \
            .withColumn("news_id", split(col("impression"), "-")[0]) \
            .withColumn("clicked", split(col("impression"), "-")[1] == "1") \
            .withColumn("interaction_type", when(col("clicked"), "click").otherwise("impression")) \
            .select("user_id", "news_id", "interaction_type", "timestamp") \
            .withColumn("article_id", col("news_id"))
        
        return impressions_df
    
    def _generate_user_profiles(self, interactions_path: str):
        """Generate comprehensive user profiles"""
        interactions_df = self.spark.read.parquet(interactions_path)
        
        # Calculate user statistics
        user_stats = interactions_df \
            .groupBy("user_id") \
            .agg(
                count("*").alias("total_interactions"),
                sum(when(col("interaction_type") == "click", 1).otherwise(0)).alias("total_clicks"),
                sum(when(col("interaction_type") == "impression", 1).otherwise(0)).alias("total_impressions"),
                countDistinct("article_id").alias("unique_articles"),
                min("timestamp").alias("first_activity"),
                max("timestamp").alias("last_activity")
            ) \
            .withColumn("ctr", col("total_clicks") / col("total_impressions")) \
            .withColumn("activity_span_days", 
                datediff(col("last_activity"), col("first_activity"))
            )
        
        return user_stats
    
    def run_real_time_processing(self):
        """Run real-time stream processing"""
        try:
            logger.info("Starting real-time processing...")
            
            # Start streaming queries
            queries = []
            
            # Process articles stream
            articles_query = self.process_news_articles_stream()
            if articles_query:
                queries.append(articles_query)
            
            # Process behaviors stream
            behaviors_query = self.process_user_behaviors_stream()
            if behaviors_query:
                queries.append(behaviors_query)
            
            # Wait for all queries to finish
            for query in queries:
                query.awaitTermination()
                
        except Exception as e:
            logger.error(f"Error in real-time processing: {e}")
    
    def run_batch_analytics(self, data_dir: str, output_dir: str):
        """Run comprehensive batch analytics"""
        try:
            logger.info("Starting batch analytics...")
            
            # 1. Process raw dataset
            self.process_mind_dataset_batch(data_dir, f"{output_dir}/processed")
            
            # 2. Train recommendation model
            model = self.train_collaborative_filtering_model(
                f"{output_dir}/processed/interactions",
                f"{output_dir}/models/collaborative_filtering"
            )
            
            # 3. Generate batch recommendations
            if model:
                self.generate_batch_recommendations(
                    f"{output_dir}/models/collaborative_filtering",
                    f"{output_dir}/processed/user_profiles",
                    f"{output_dir}/recommendations"
                )
            
            # 4. Analyze user engagement
            self.analyze_user_engagement(
                f"{output_dir}/processed/behaviors",
                f"{output_dir}/analytics/user_engagement"
            )
            
            # 5. Analyze content popularity
            self.analyze_content_popularity(
                f"{output_dir}/processed/articles",
                f"{output_dir}/processed/interactions",
                f"{output_dir}/analytics/content_popularity"
            )
            
            logger.info("Batch analytics completed successfully!")
            
        except Exception as e:
            logger.error(f"Error in batch analytics: {e}")
    
    def stop(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()

def main():
    """Main execution function"""
    processor = MindSparkProcessor()
    
    try:
        processor.initialize_spark()
        
        # Get configuration from environment
        mode = os.getenv('SPARK_MODE', 'batch')  # 'batch' or 'stream'
        data_dir = os.getenv('MIND_DATA_DIR', '/opt/data/mind')
        output_dir = os.getenv('SPARK_OUTPUT_DIR', '/opt/data/output')
        
        if mode == 'stream':
            # Run real-time processing
            processor.run_real_time_processing()
        else:
            # Run batch processing
            processor.run_batch_analytics(data_dir, output_dir)
            
    except KeyboardInterrupt:
        logger.info("Processing interrupted by user")
    except Exception as e:
        logger.error(f"Processing failed: {e}")
    finally:
        processor.stop()

if __name__ == "__main__":
    main()