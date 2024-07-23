#CSV to JSON Conversion
import csv
from pymongo import MongoClient

client=MongoClient("mongodb://localhost:27017/") 
db = client['datalake']
collection = db['user_data']

def insert_data(name, email, age, hobby):
    data = {
        'name': name,
        'email': email,
        'age': age,
        'hobby': hobby
    }
    collection.insert_one(data)
