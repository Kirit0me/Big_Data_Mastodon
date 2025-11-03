"""
Enhanced sentiment analysis with aspect/topic detection
This addresses the question: "Sentiment towards WHAT?"
"""
import re
from textblob import TextBlob
from collections import defaultdict
import spacy
from typing import Dict, List, Tuple

class AspectBasedSentimentAnalyzer:
    """
    Analyzes sentiment with context about what the sentiment is directed towards
    """
    
    def __init__(self):
        # Load spacy for entity/noun extraction
        try:
            self.nlp = spacy.load("en_core_web_sm")
        except:
            print("Installing spacy model...")
            import subprocess
            subprocess.run(["python", "-m", "spacy", "download", "en_core_web_sm"])
            self.nlp = spacy.load("en_core_web_sm")
    
    def clean_html(self, text: str) -> str:
        """Remove HTML tags"""
        return re.sub(r'<[^>]+>', '', text)
    
    def extract_aspects(self, text: str) -> List[str]:
        """
        Extract aspects (topics/entities) from text
        Returns nouns, named entities, and hashtag-related concepts
        """
        doc = self.nlp(text)
        aspects = []
        
        # Named entities (people, organizations, locations, etc.)
        for ent in doc.ents:
            if ent.label_ in ['PERSON', 'ORG', 'GPE', 'PRODUCT', 'EVENT']:
                aspects.append((ent.text.lower(), ent.label_))
        
        # Important nouns and noun phrases
        for chunk in doc.noun_chunks:
            # Filter out pronouns and very short chunks
            if len(chunk.text) > 3 and chunk.root.pos_ == 'NOUN':
                aspects.append((chunk.text.lower(), 'TOPIC'))
        
        return aspects
    
    def analyze_sentiment_by_aspect(self, text: str, hashtags: List[str]) -> Dict:
        """
        Analyze sentiment with aspects/targets
        
        Returns:
        {
            'overall_sentiment': float,
            'overall_subjectivity': float,
            'aspect_sentiments': [
                {
                    'aspect': str,
                    'aspect_type': str,
                    'sentiment': float,
                    'context': str
                }
            ],
            'hashtag_context': {
                'hashtag': {
                    'sentiment': float,
                    'related_aspects': []
                }
            },
            'sentiment_summary': str
        }
        """
        clean_text = self.clean_html(text)
        
        # Overall sentiment
        blob = TextBlob(clean_text)
        overall_sentiment = blob.sentiment.polarity
        overall_subjectivity = blob.sentiment.subjectivity
        
        # Extract aspects
        aspects = self.extract_aspects(clean_text)
        
        # Analyze sentiment around each aspect
        aspect_sentiments = []
        doc = self.nlp(clean_text)
        
        for aspect, aspect_type in aspects:
            # Find sentences containing this aspect
            aspect_sentences = [
                sent.text for sent in doc.sents 
                if aspect in sent.text.lower()
            ]
            
            if aspect_sentences:
                # Get sentiment of sentences containing the aspect
                aspect_sentiment = sum(
                    TextBlob(sent).sentiment.polarity 
                    for sent in aspect_sentences
                ) / len(aspect_sentences)
                
                aspect_sentiments.append({
                    'aspect': aspect,
                    'aspect_type': aspect_type,
                    'sentiment': round(aspect_sentiment, 3),
                    'context': aspect_sentences[0][:100]  # First sentence snippet
                })
        
        # Analyze sentiment in context of hashtags
        hashtag_context = {}
        for tag in hashtags:
            # Find sentences mentioning the hashtag or related to it
            tag_sentences = [
                sent.text for sent in doc.sents 
                if tag.lower() in sent.text.lower()
            ]
            
            if tag_sentences:
                tag_sentiment = sum(
                    TextBlob(sent).sentiment.polarity 
                    for sent in tag_sentences
                ) / len(tag_sentences)
            else:
                tag_sentiment = overall_sentiment  # Fallback to overall
            
            # Find aspects mentioned with this hashtag
            related_aspects = [
                a['aspect'] for a in aspect_sentiments 
                if any(tag.lower() in sent.lower() for sent in [a['context']])
            ]
            
            hashtag_context[tag] = {
                'sentiment': round(tag_sentiment, 3),
                'related_aspects': related_aspects[:3]  # Top 3
            }
        
        # Generate summary
        sentiment_summary = self._generate_summary(
            overall_sentiment, 
            aspect_sentiments, 
            hashtag_context
        )
        
        return {
            'overall_sentiment': round(overall_sentiment, 3),
            'overall_subjectivity': round(overall_subjectivity, 3),
            'aspect_sentiments': aspect_sentiments,
            'hashtag_context': hashtag_context,
            'sentiment_summary': sentiment_summary,
            'dominant_aspects': self._get_dominant_aspects(aspect_sentiments)
        }
    
    def _generate_summary(self, overall: float, aspects: List, hashtags: Dict) -> str:
        """Generate human-readable sentiment summary"""
        if overall > 0.3:
            tone = "positive"
        elif overall < -0.3:
            tone = "negative"
        else:
            tone = "neutral"
        
        if aspects:
            top_aspect = max(aspects, key=lambda x: abs(x['sentiment']))
            return f"{tone.capitalize()} tone; Most pronounced towards '{top_aspect['aspect']}' ({top_aspect['sentiment']:.2f})"
        else:
            return f"{tone.capitalize()} tone overall"
    
    def _get_dominant_aspects(self, aspect_sentiments: List) -> List[Dict]:
        """Get top 3 aspects by sentiment intensity"""
        sorted_aspects = sorted(
            aspect_sentiments, 
            key=lambda x: abs(x['sentiment']), 
            reverse=True
        )
        return sorted_aspects[:3]


# Updated UDF for Spark
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType, ArrayType

def create_enhanced_sentiment_udf():
    """Create UDF for use in Spark pipeline"""
    analyzer = AspectBasedSentimentAnalyzer()
    
    sentiment_schema = StructType([
        StructField("overall_sentiment", FloatType()),
        StructField("overall_subjectivity", FloatType()),
        StructField("sentiment_summary", StringType()),
        StructField("top_aspect", StringType()),
        StructField("top_aspect_sentiment", FloatType()),
    ])
    
    @udf(returnType=sentiment_schema)
    def enhanced_sentiment_udf(content, hashtags):
        """Enhanced sentiment analysis UDF"""
        if not content:
            return (0.0, 0.0, "No content", None, 0.0)
        
        try:
            result = analyzer.analyze_sentiment_by_aspect(content, hashtags or [])
            
            top_aspect = None
            top_aspect_sentiment = 0.0
            
            if result['dominant_aspects']:
                top = result['dominant_aspects'][0]
                top_aspect = top['aspect']
                top_aspect_sentiment = top['sentiment']
            
            return (
                float(result['overall_sentiment']),
                float(result['overall_subjectivity']),
                result['sentiment_summary'],
                top_aspect,
                float(top_aspect_sentiment)
            )
        except Exception as e:
            return (0.0, 0.0, f"Error: {str(e)}", None, 0.0)
    
    return enhanced_sentiment_udf


# Example usage
if __name__ == "__main__":
    analyzer = AspectBasedSentimentAnalyzer()
    
    # Test post
    test_content = """
    <p>I absolutely love the new iPhone 15! The camera is incredible. 
    However, Apple's pricing strategy is getting ridiculous. 
    Tim Cook needs to reconsider. #Apple #iPhone15 #TechReview</p>
    """
    
    result = analyzer.analyze_sentiment_by_aspect(
        test_content, 
        ['Apple', 'iPhone15', 'TechReview']
    )
    
    print("=== Sentiment Analysis Results ===")
    print(f"Overall: {result['overall_sentiment']} ({result['sentiment_summary']})")
    print(f"\nAspect-level sentiments:")
    for aspect in result['aspect_sentiments']:
        print(f"  - {aspect['aspect']} ({aspect['aspect_type']}): {aspect['sentiment']}")
    
    print(f"\nHashtag context:")
    for tag, data in result['hashtag_context'].items():
        print(f"  #{tag}: sentiment={data['sentiment']}, aspects={data['related_aspects']}")