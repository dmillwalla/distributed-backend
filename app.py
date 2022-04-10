from collections import defaultdict
import os
import datetime
import time

from flask import Flask, request, make_response
from flask_cors import CORS

from pymongo import MongoClient

import random


app = Flask(__name__)
cors = CORS(app, resources={r"/*": {"origins": "*"}})

client = MongoClient(os.getenv("MONGO_CONNECTION_STRING"))
db = client.test    #Select the database

loc_string = os.getenv('LOC', 'GLBL')

EU_COUNTRIES = ["UK", "IRL", "FRA", "ESP"]
NA_COUNTRIES = ["MEX", "USA", "CAN"]

@app.route('/')
def root():
    return make_response("Works", 200)

@app.after_request
def add_loc_headers(resp):
    resp.headers['X-LOC']=loc_string
    return resp

def get_random_index_in_range(start, end):
    return random.randint(start, end)

@app.route('/addAthleteEntry', methods=['POST'])
def add_entry():
    req_body = request.get_json()
    athlete_id = req_body['ID']
    city = req_body['city']
    country = req_body['country']
    timestamp = req_body['timestamp']

    namespace = "GLBL"

    if country in EU_COUNTRIES:
        namespace = "EU"
    
    if country in NA_COUNTRIES:
        namespace = "NA"

    timestamp_obj = datetime.datetime.fromtimestamp(float(timestamp))
    athlete_obj = {}
    athlete_obj["athlete_id"] = athlete_id
    athlete_obj["city"] = city
    athlete_obj["country"] = country
    athlete_obj["timestamp"] = timestamp

    #check if timestamp is at least >48 hours from current timestamp
    db[namespace + "-athletes"].insert_one(athlete_obj)

@app.route('/scheduleDoping', methods=['POST'])
def schedule_doping():
    current_timestamp = time.time()
    available_athletes = db[loc_string + "-athletes"].find({"timestamp": {"$gt": current_timestamp}})
    athlete_time_map = defaultdict(lambda: set())
    athlete_avl_map = {}
    agent_time_map = defaultdict(lambda: set())
    athletes_list = []
    athletes_names_list = set()
    for each_avl_athlete in available_athletes:
        datetime_obj = datetime.datetime.fromtimestamp(each_avl_athlete["timestamp"])
        key = each_avl_athlete["athlete_id"]
        value = datetime_obj.strftime("%d") + "-" + datetime_obj.strftime("%H")
        athlete_time_map[key].add(each_avl_athlete)
        athletes_list.append(key+"-"+ value)
        athlete_avl_map[key+"-"+value] = each_avl_athlete
        athletes_names_list.add(key)

    available_agents = db[loc_string + "-agents"].find()
    all_agents_name = set()

    for each_agent in available_agents:
        all_agents_name.add(each_agent["agent_id"])

    agent_slots = db[loc_string + "-agentslots"].find({"timestamp": {"$gt": current_timestamp}})

    for each_slot in agent_slots:
        datetime_obj = datetime.datetime.fromtimestamp(each_slot["timestamp"])
        key = datetime_obj.strftime("%d") + "-" + datetime_obj.strftime("%H")
        agent_time_map[key].add(each_slot["agent_id"])

    for each_athlete in athletes_names_list:
        random_int = get_random_index_in_range(0, 9)
        if random_int > 5:
            time_slots = athlete_time_map[each_athlete]
            for each_time_slot in time_slots:
                agents_booked = agent_time_map[key]
                free_agents = all_agents_name - agents_booked
                if len(free_agents) > 0:
                    day, hour = each_time_slot.split("-")
                    avl_agent = free_agents.iterator().next()
                    timestamp = athlete_avl_map[each_athlete+"-"+each_time_slot]
                    agent_slot_obj = {"agent_id":avl_agent, "athlete_id": each_athlete, "day": day, "hour": hour, "timestamp": timestamp }
                    db[loc_string + "-agentslots"].insert_one(agent_slot_obj)
                    agent_time_map[each_time_slot].add(avl_agent)


@app.route('/getUpcomingAgentSchedule/<agent_id>', methods=['GET'])
def upcoming_agent_schedule(agent_id):
    current_timestamp = time.time()
    agent_slots = db[loc_string + "-agentslots"]\
        .find({ "$and": [{"timestamp": {"$gt": current_timestamp}}, 
                        {"agent_id": {"$eq": agent_id}} ] })

if __name__ == '__main__':
    app.run(host="127.0.0.1", port = 8080, debug=True)