import pymongo
from requests_futures.sessions import FuturesSession
import json
import sched, time
import os

session = FuturesSession()
counter = 0
TOKEN = os.environ.get('MONGODB_TOKEN')
client = pymongo.MongoClient(f"mongodb+srv://admin:{TOKEN}@cluster0.zaufo.mongodb.net/albion?retryWrites=true&w=majority")
db = client.albion
players_db = db.albion

def get_participants(data):
    fights = json.loads(data.content.decode())

    all_players = []

    for fight in fights:
        participants = fight["Participants"]
        for part in participants:            
            player_info = { "_id": part["Id"], "name": part["Name"], "time": fight["TimeStamp"] }
            all_players.append(player_info)
        player_info = { "_id": fight["Victim"]["Id"], "name": fight["Victim"]["Name"], "time": fight["TimeStamp"] }
        all_players.append(player_info)

    try:
        players_db.insert_many(all_players, ordered=False) # ordered=False to soft ignore duplicates
    except pymongo.errors.BulkWriteError:
        pass # dont care, didnt ask
    
    
    
s = sched.scheduler(time.time, time.sleep)
def get_fights_data(sc, counter): 
    for i in range(0, 250, 50):
        data = session.get(f"https://gameinfo.albiononline.com/api/gameinfo/events?limit=50&offset={i}").result()
        if data.status_code != 200:
            print("error: ", data.status_code)
            continue
        get_participants(data)
    print(counter)
    counter += 1
    s.enter(60, 1, get_fights_data, (sc, counter, ))

s.enter(60, 1, get_fights_data, (s, counter, ))
s.run()
    
print("done")

