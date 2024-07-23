import pymongo

# Connect to the MongoDB database
client = pymongo.MongoClient("mongodb://localhost:27017/")

# Choose the collection
db = client["datalake"]
collection = db["user_data"]

# Define the projection to exclude the "_id" field
projection = {"_id": 0}

def find_userdata(username):
    # Execute a query with the projection
    cursor = collection.find({"name":username}, projection)
    data = []

    for document in cursor:
        data.append(document)

    return data