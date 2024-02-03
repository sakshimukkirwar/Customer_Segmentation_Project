from nltk.sentiment import SentimentIntensityAnalyzer
import nltk
from nltk.tokenize import word_tokenize
from nltk.probability import FreqDist
from nltk.corpus import stopwords
from pyspark.sql.types import StringType, ArrayType, MapType
from pyspark.sql.functions import col, udf

nltk.download('vader_lexicon')
nltk.download('stopwords')
nltk.download('punkt')
sia = SentimentIntensityAnalyzer()
stop_words = set(stopwords.words('english'))


@udf(StringType())
def get_sentiment(text):
    sentiment_score = sia.polarity_scores(text)["compound"]
    if sentiment_score >= 0.05:
        return "positive"
    elif sentiment_score <= -0.05:
        return "negative"
    else:
        return "neutral"


@udf(ArrayType(StringType()))
def tokenize_and_get_top_words(text, sample_size=0.0001):
    tokens = word_tokenize(text)
    tokens = [word.lower() for word in tokens if word.isalpha()]
    tokens = [word for word in tokens if word not in stop_words]
    freq_dist = FreqDist(tokens)
    top_words = [word  for word, k in freq_dist.most_common(10)]
    return top_words

