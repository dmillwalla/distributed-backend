import os
import datetime
import time

from flask import Flask, request, make_response
from flask_cors import CORS

from pymongo import MongoClient


app = Flask(__name__)
cors = CORS(app, resources={r"/*": {"origins": "*"}})

client = MongoClient(os.getenv("MONGO_CONNECTION_STRING"))
db = client.test    #Select the database

@app.route('/')
def root():
    return make_response("Works", 200)

@app.route('/addEntry', methods=['POST'])
def add_entry():
    req_body = request.get_json()
    athlete_id = req_body['ID']
    city = req_body['city']
    country = req_body['country']
    timestamp = req_body['timestamp']

    timestamp_obj = datetime.datetime.fromtimestamp(float(timestamp)/1000.)
    athlete_obj = {}
    athlete_obj["athlete_id"] = athlete_id
    athlete_obj["city"] = city
    athlete_obj["country"] = country
    athlete_obj["timestamp"] = timestamp

    #check if timestamp is at least >48 hours from current timestamp
    db.athletes.insert_one(athlete_obj)
    

if __name__ == '__main__':
    app.run(host="127.0.0.1", port = 8080, debug=True)