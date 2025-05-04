import os
from pymongo import MongoClient
import redis
import msgpack  # Use MessagePack for compact binary serialization
import zlib  # For data compression and decompression

# MongoDB connection
MONGO_URI = "mongodb+srv://2004:2005@cluster0.6vdid.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(MONGO_URI)
db = client["telegram_bot"]
leaderboard_collection = db["leaderboard"]

# Redis connection for caching
redis_client = redis.StrictRedis(host="localhost", port=6379, db=0)

def compress_data(data):
    """
    Compress data using zlib.
    """
    return zlib.compress(msgpack.packb(data))

def decompress_data(data):
    """
    Decompress data using zlib.
    """
    return msgpack.unpackb(zlib.decompress(data), raw=False)

def load_leaderboard():
    """
    Load leaderboard data with caching, compression, and MessagePack serialization.
    """
    cache_key = "leaderboard"
    cached_leaderboard = redis_client.get(cache_key)
    if cached_leaderboard:
        return decompress_data(cached_leaderboard)

    # Fetch leaderboard from MongoDB with projection
    leaderboard = {}
    for entry in leaderboard_collection.find({}, {"user_id": 1, "score": 1}):  # Use projection
        leaderboard[entry["user_id"]] = entry["score"]

    # Cache the leaderboard with compression
    redis_client.set(cache_key, compress_data(leaderboard), ex=3600)  # Cache for 1 hour
    return leaderboard

def save_leaderboard(leaderboard):
    """
    Save leaderboard data to MongoDB and invalidate cache.
    """
    # Batch insert leaderboard data
    leaderboard_collection.delete_many({})
    leaderboard_data = [{"user_id": user_id, "score": score} for user_id, score in leaderboard.items()]
    if leaderboard_data:
        leaderboard_collection.insert_many(leaderboard_data)

    # Invalidate and update cache
    redis_client.delete("leaderboard")
    redis_client.set("leaderboard", compress_data(leaderboard), ex=3600)  # Cache for 1 hour

def add_score(user_id, score):
    """
    Add or update a user's score with compression and cache invalidation.
    """
    current_score = leaderboard_collection.find_one({"user_id": user_id}, {"score": 1})  # Use projection
    if current_score:
        new_score = current_score["score"] + score
        leaderboard_collection.update_one({"user_id": user_id}, {"$set": {"score": new_score}})
    else:
        leaderboard_collection.insert_one({"user_id": user_id, "score": score})

    # Invalidate cache for leaderboard and user score
    redis_client.delete("leaderboard")
    redis_client.delete(f"user_score:{user_id}")

def get_top_scores(n=20):
    """
    Retrieve the top n scores with caching, compression, and MessagePack serialization.
    """
    cache_key = f"top_scores_{n}"
    cached_scores = redis_client.get(cache_key)
    if cached_scores:
        return decompress_data(cached_scores)

    # Query MongoDB for top scores with projection
    top_scores = leaderboard_collection.find({}, {"user_id": 1, "score": 1}).sort("score", -1).limit(n)
    result = [(entry["user_id"], entry["score"]) for entry in top_scores]

    # Cache the top scores with compression
    redis_client.set(cache_key, compress_data(result), ex=3600)  # Cache for 1 hour
    return result

def get_user_score(user_id):
    """
    Retrieve a user's score with caching, compression, and MessagePack serialization.
    """
    cache_key = f"user_score:{user_id}"
    cached_score = redis_client.get(cache_key)
    if cached_score:
        return decompress_data(cached_score)

    # Query MongoDB for user score with projection
    user = leaderboard_collection.find_one({"user_id": user_id}, {"score": 1})  # Use projection
    score = user["score"] if user else 0

    # Cache the user score with compression
    redis_client.set(cache_key, compress_data(score), ex=3600)  # Cache for 1 hour
    return score
