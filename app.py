from collections import defaultdict
import os
import datetime
import time

from flask import Flask, request, make_response, jsonify
from flask_cors import CORS

from pymongo import MongoClient
from bson.json_util import dumps

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
    insert_result = db["test"].insert_one({"x": 1})
    print(insert_result.inserted_id)

    db["test"].update_one({'_id': insert_result.inserted_id},{"$set":{"y": 2}})

    return_obj = {}
    return_obj["status"] = "Works"
    return_obj["stuff"] = str(insert_result.inserted_id)
    return make_response(jsonify(return_obj), 200)

@app.route('/test')
def test():

    return_obj = {}
    return_obj["status"] = "Works"
    return_obj["stuff"] = str(time.time())
    return make_response(jsonify(return_obj), 200)

@app.after_request
def add_loc_headers(resp):
    resp.headers['X-LOC']=loc_string
    return resp

def get_random_index_in_range(start, end):
    return random.randint(start, end)

@app.route('/addAthleteEntry', methods=['POST'])
def add_entry():
    req_body = request.get_json()
    athlete_id = req_body['athlete_id']
    city = req_body['city']
    country = req_body['country']
    timestamp = req_body['timestamp']
    
    current_timestamp = time.time()

    namespace = "GLBL"

    if country in EU_COUNTRIES:
        namespace = "EU"
    
    if country in NA_COUNTRIES:
        namespace = "NA"

    timestamp_obj = datetime.datetime.fromtimestamp(int(float(timestamp)))
    datetime_obj = datetime.datetime.fromtimestamp(int(float(timestamp)))

    day_of_month = datetime_obj.strftime("%d") 
    time_of_day = datetime_obj.strftime("%H")
    athlete_obj = {}
    athlete_obj["athlete_id"] = athlete_id
    athlete_obj["city"] = city
    athlete_obj["country"] = country
    athlete_obj["timestamp"] = float(timestamp)
    athlete_obj["day"] = day_of_month
    athlete_obj["time"] = time_of_day

    #check if timestamp is at least >72 hours from current timestamp
    if float(timestamp) - current_timestamp < 60 * 60 * 24 * 3:
        return_obj = {}
        return_obj["status"] = "Failure"
        return_obj["reason"] = "Can't update anything within 3 days. Stay wherever you are"
        return make_response(jsonify(return_obj), 200)#reject update

    #check if timestamp is not >240 hours from current timestamp
    if float(timestamp) - current_timestamp > 60 * 60 * 24 * 10:
        return_obj = {}
        return_obj["status"] = "Failure"
        return_obj["reason"] = "Can't update anything 10 days in the future. Stay wherever you are"
        return make_response(jsonify(return_obj), 200)#reject update

    tracker_obj = db["athlete-avl-ops"].insert_one({"timestamp": current_timestamp, "athlete_id": athlete_id, "op_status": "PENDING"})

    # For Athletes

    na_athl_delete_id = db["NA-athletes"].find_one({ "$and": [{"athlete_id": {"$eq": athlete_id}}, 
                        {"day": {"$eq": day_of_month}} ] })
    db["NA-athletes"].delete_one({ "$and": [{"athlete_id": {"$eq": athlete_id}}, 
                        {"day": {"$eq": day_of_month}} ] })
    if na_athl_delete_id is not None:
        db["athlete-avl-ops"].update_one({'_id': tracker_obj.inserted_id},{"$set":{"NA-athletes_delete_id": na_athl_delete_id}})
    

    eu_athl_delete_id = db["EU-athletes"].find_one({ "$and": [{"athlete_id": {"$eq": athlete_id}}, 
                        {"day": {"$eq": day_of_month}} ] })
    db["EU-athletes"].delete_one({ "$and": [{"athlete_id": {"$eq": athlete_id}}, 
                        {"day": {"$eq": day_of_month}} ] })
    if eu_athl_delete_id is not None:
        db["athlete-avl-ops"].update_one({'_id': tracker_obj.inserted_id},{"$set":{"EU-athletes_delete_id": eu_athl_delete_id}})
    


    # For Agents

    na_agent_delete_id = db["NA-agentslots"].find_one({ "$and": [{"athlete_id": {"$eq": athlete_id}}, 
                        {"day": {"$eq": day_of_month}} ] })
    db["NA-agentslots"].delete_one({ "$and": [{"athlete_id": {"$eq": athlete_id}}, 
                        {"day": {"$eq": day_of_month}} ] })
    if na_agent_delete_id is not None:
        db["athlete-avl-ops"].update_one({'_id': tracker_obj.inserted_id},{"$set":{"NA-agentslots_delete_id": na_agent_delete_id}})

    eu_agent_delete_id = db["EU-athletes"].find_one({ "$and": [{"athlete_id": {"$eq": athlete_id}}, 
                        {"day": {"$eq": day_of_month}} ] })
    db["EU-agentslots"].delete_one({ "$and": [{"athlete_id": {"$eq": athlete_id}}, 
                        {"day": {"$eq": day_of_month}} ] })
    if eu_agent_delete_id is not None:
        db["athlete-avl-ops"].update_one({'_id': tracker_obj.inserted_id},{"$set":{"EU-agentslots_delete_id": eu_agent_delete_id}})
    


    insert_result = db[namespace + "-athletes"].insert_one(athlete_obj)
    db["athlete-avl-ops"].update_one({'_id': tracker_obj.inserted_id},{"$set":{"insert_"+ namespace + "-athletes_id": insert_result.inserted_id,"op_status": "FINISHED"}})
    print("insert_result.inserted_id", insert_result.inserted_id)
    athlete_obj["ID"] = str(insert_result.inserted_id)
    return make_response(dumps(athlete_obj), 200)

@app.route('/scheduleDoping', methods=['POST'])
def schedule_doping():
    current_timestamp = time.time()
    tracker_obj = db["scheduler-ops"].insert_one({"timestamp": current_timestamp, "op_status": "PENDING"})
    three_days = 60 * 60 * 24 * 3
    available_athletes = db[loc_string + "-athletes"].find({"timestamp": {"$gt": current_timestamp + three_days}})
    athlete_map = defaultdict(lambda: [])
    athlete_avl_map = {}
    agent_time_map = defaultdict(lambda: set())
    athlete_scheduled_time_map = defaultdict(lambda: set())
    athletes_list = []
    athletes_names_list = set()
    for each_avl_athlete in available_athletes:
        datetime_obj = datetime.datetime.fromtimestamp(int(float(each_avl_athlete["timestamp"])))
        key = each_avl_athlete["athlete_id"]
        value = datetime_obj.strftime("%d") + "-" + datetime_obj.strftime("%H")
        athlete_map[key].append(each_avl_athlete)
        athletes_list.append(key+"-"+ value)
        athlete_avl_map[key+"-"+value] = each_avl_athlete
        athletes_names_list.add(key)

    available_agents = db[loc_string + "-agents"].find()
    all_agents_name = set()

    for each_agent in available_agents:
        all_agents_name.add(each_agent["agent_id"])

    agent_slots = db[loc_string + "-agentslots"].find({"timestamp": {"$gt": current_timestamp}})

    for each_slot in agent_slots:
        datetime_obj = datetime.datetime.fromtimestamp(int(float(each_slot["timestamp"])))
        key = datetime_obj.strftime("%d") + "-" + datetime_obj.strftime("%H")
        agent_time_map[key].add(each_slot["agent_id"])
        athlete_scheduled_time_map[key].add(each_slot["athlete_id"])

    for each_athlete in athletes_names_list:
        random_int = get_random_index_in_range(0, 9)
        # if random_int > 5:
        if random_int > -1:
            athlete_slots = athlete_map[each_athlete]
            for each_time_slot in athlete_slots:
                timestamp = each_time_slot["timestamp"]
                datetime_obj = datetime.datetime.fromtimestamp(int(float(timestamp)))
                key = datetime_obj.strftime("%d") + "-" + datetime_obj.strftime("%H")
                if len(athlete_scheduled_time_map[key]) > 0:
                    continue # don't double book athlete. Can also break to ignore athlete for double drug test in a week
                agents_booked = agent_time_map[key]
                free_agents = all_agents_name - agents_booked
                if len(free_agents) > 0:
                    day, hour = key.split("-")
                    avl_agent = next(iter(free_agents))
                    agent_slot_obj = {"agent_id":avl_agent, "athlete_id": each_athlete, "day": day, "hour": hour, "timestamp": timestamp }
                    insert_op = db[loc_string + "-agentslots"].insert_one(agent_slot_obj)
                    db["scheduler-ops"].update_one({'_id': tracker_obj.inserted_id},{"$push":{"insert_"+ loc_string + "-agentslots_id": insert_op.inserted_id}})
                    agent_time_map[key].add(avl_agent)
                    break

    return_obj = {}
    return_obj["status"] = "Success"
    return_obj["details"] = "Scheduling completed"

    db["scheduler-ops"].update_one({'_id': tracker_obj.inserted_id},{"$set":{"op_status": "FINISHED"}})
    
    return make_response(jsonify(return_obj), 200)

@app.route('/fixAthleteEntryFailure')
def fix_athlete_entry_failure():
    current_timestamp = time.time()
    past_hour = 60 * 60
    pending_obj = db["athlete-avl-ops"].find_one({"$and":[{"op_status": {"$eq": "PENDING"}},{"timestamp": {"$lt": current_timestamp - past_hour}}]})
    
    if pending_obj is not None:
        na_athl = pending_obj["NA-athletes_delete_id"]
        if na_athl is not None:
            db["NA-athletes"].insert_one(na_athl)
        eu_athl = pending_obj["EU-athletes_delete_id"]
        if eu_athl is not None:
            db["EU-athletes"].insert_one(eu_athl)
        na_agent = pending_obj["NA-athletes_delete_id"]
        if na_agent is not None:
            db["NA-agentslots"].insert_one(na_agent)
        eu_agent = pending_obj["EU-athletes_delete_id"]
        if eu_agent is not None:
            db["EU-agentslots"].insert_one(eu_agent)
        na_inserted_id = pending_obj["insert_NA-athletes_id"]
        if na_inserted_id is not None:
            db["NA-athletes"].delete_one({'_id': na_inserted_id})
        eu_inserted_id = pending_obj["insert_EU-athletes_id"]
        if eu_inserted_id is not None:
            db["EU-athletes"].delete_one({'_id': eu_inserted_id})
        db["athlete-avl-ops"].update_one({'_id': pending_obj["_id"]},{"$set":{"op_status": "RECONCILED"}})
    

    return_obj = {}
    return_obj["status"] = "Success"
    return_obj["details"] = "Failure Reconciliation Completed"
    
    return make_response(jsonify(return_obj), 200)


@app.route('/getUpcomingAgentSchedule/<agent_id>', methods=['GET'])
def upcoming_agent_schedule(agent_id):
    current_timestamp = time.time()
    agent_slots = db[loc_string + "-agentslots"]\
        .find({ "$and": [{"timestamp": {"$gt": current_timestamp}}, 
                        {"agent_id": {"$eq": agent_id}} ] })

    return make_response(dumps(agent_slots), 200)

if __name__ == '__main__':
    app.run(host="127.0.0.1", port = 8080, debug=True)