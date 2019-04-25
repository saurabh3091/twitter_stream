# Twitter Streaming Analysis

### Problem Statement
Stream tweets from Twitter API for N hours and store as JSON.

Find top 10 trending words and hashtags.
### Prerequisites

Python 3.6,
Spark 2.3.1

### Start Twitter Stream
```
# put twitter api credentials[consumer_key, consumer_secret, access_token, access_token_secret] at resources/config and run
# The script will run for N hours defined in config
python twitter_stream.py

# tweets will be saved at resources/tweets.json
```
## Run on Analysis Spark job on Local

```
spark-submit --master "local[*]"  path_to_repository/spark/analyze_tweets.py --json json_path
```

## Output
Will be stored as output.json file at the home directory of repo.