import networkx as nx
from kafka import KafkaConsumer
import json
import psycopg2
from datetime import datetime, timedelta
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HashtagGraphAnalytics:
    def __init__(self):
        self.G = nx.Graph()
        self.db_conn = psycopg2.connect(
            dbname="mastodon_analytics",
            user="mastodon",
            password="your_password",
            host="localhost"
        )
        self.create_tables()
        
    def create_tables(self):
        """Create tables for graph metrics"""
        with self.db_conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS hashtag_centrality (
                    hashtag VARCHAR(255) PRIMARY KEY,
                    degree_centrality FLOAT,
                    betweenness_centrality FLOAT,
                    pagerank FLOAT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS hashtag_communities (
                    hashtag VARCHAR(255),
                    community_id INT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (hashtag, community_id)
                )
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_hashtag_network (
                    user_name VARCHAR(255),
                    hashtag VARCHAR(255),
                    co_occurrence_count INT DEFAULT 1,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (user_name, hashtag)
                )
            """)
            self.db_conn.commit()
    
    def build_graph_from_db(self, time_window_hours=24):
        """Build co-occurrence graph from recent posts"""
        logger.info(f"Building graph from last {time_window_hours} hours")
        
        with self.db_conn.cursor() as cur:
            # Get posts from last N hours
            cur.execute("""
                SELECT user_name, hashtag 
                FROM posts 
                WHERE processing_time > NOW() - INTERVAL '%s hours'
            """, (time_window_hours,))
            
            posts_by_user = {}
            for user, hashtag in cur.fetchall():
                if user not in posts_by_user:
                    posts_by_user[user] = []
                posts_by_user[user].append(hashtag)
        
        # Build co-occurrence graph
        self.G.clear()
        for user, hashtags in posts_by_user.items():
            # Add edges between hashtags used together
            for i, tag1 in enumerate(hashtags):
                for tag2 in hashtags[i+1:]:
                    if self.G.has_edge(tag1, tag2):
                        self.G[tag1][tag2]['weight'] += 1
                    else:
                        self.G.add_edge(tag1, tag2, weight=1)
        
        logger.info(f"Graph built: {self.G.number_of_nodes()} nodes, {self.G.number_of_edges()} edges")
    
    def compute_centrality_metrics(self):
        """Compute various centrality measures"""
        if self.G.number_of_nodes() == 0:
            logger.warning("Empty graph, skipping centrality computation")
            return
        
        logger.info("Computing centrality metrics...")
        
        # Degree centrality
        degree_cent = nx.degree_centrality(self.G)
        
        # Betweenness centrality
        betweenness_cent = nx.betweenness_centrality(self.G, weight='weight')
        
        # PageRank
        pagerank = nx.pagerank(self.G, weight='weight')
        
        # Store in database
        with self.db_conn.cursor() as cur:
            for node in self.G.nodes():
                cur.execute("""
                    INSERT INTO hashtag_centrality 
                    (hashtag, degree_centrality, betweenness_centrality, pagerank, updated_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (hashtag) 
                    DO UPDATE SET 
                        degree_centrality = EXCLUDED.degree_centrality,
                        betweenness_centrality = EXCLUDED.betweenness_centrality,
                        pagerank = EXCLUDED.pagerank,
                        updated_at = NOW()
                """, (node, degree_cent[node], betweenness_cent[node], pagerank[node]))
            self.db_conn.commit()
        
        logger.info("Centrality metrics saved to database")
    
    def detect_communities(self):
        """Detect communities using Louvain algorithm"""
        if self.G.number_of_nodes() == 0:
            return
        
        logger.info("Detecting communities...")
        
        try:
            communities = nx.community.louvain_communities(self.G, weight='weight')
            
            with self.db_conn.cursor() as cur:
                # Clear old communities
                cur.execute("DELETE FROM hashtag_communities")
                
                # Insert new communities
                for comm_id, community in enumerate(communities):
                    for hashtag in community:
                        cur.execute("""
                            INSERT INTO hashtag_communities (hashtag, community_id, updated_at)
                            VALUES (%s, %s, NOW())
                        """, (hashtag, comm_id))
                self.db_conn.commit()
            
            logger.info(f"Detected {len(communities)} communities")
        except Exception as e:
            logger.error(f"Community detection failed: {e}")
    
    def run_periodic_analysis(self, interval_seconds=300):
        """Run graph analysis periodically"""
        logger.info(f"Starting periodic analysis (every {interval_seconds}s)")
        
        while True:
            try:
                self.build_graph_from_db(time_window_hours=24)
                self.compute_centrality_metrics()
                self.detect_communities()
                
                logger.info(f"Analysis complete. Sleeping for {interval_seconds}s...")
                time.sleep(interval_seconds)
                
            except Exception as e:
                logger.error(f"Analysis failed: {e}")
                time.sleep(60)

if __name__ == "__main__":
    analytics = HashtagGraphAnalytics()
    analytics.run_periodic_analysis(interval_seconds=300)  # Run every 5 minutes