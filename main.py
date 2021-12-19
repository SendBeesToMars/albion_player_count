from requests_futures.sessions import FuturesSession
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from datetime import datetime
import pymongo
import json
import sched, time
import os
import os.path

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = '1QwgS0Jr5HjaIWsfBKnstTQNdfxGQJyO5wm0Y04jLdbU'
SAMPLE_RANGE_NAME = 'A1:A'

session = FuturesSession()
counter = 0
TOKEN = os.environ.get('MONGODB_TOKEN')
client = pymongo.MongoClient(f"mongodb+srv://admin:{TOKEN}@cluster0.ufd5a.mongodb.net/albion?retryWrites=true&w=majority")
db = client.albion
players_db = db.albion
players_db.delete_many({}) # deletes all data in collection

previous_time = int(datetime.now().strftime("%H")) - 1
if (previous_time == -1):
    previous_time = 59
old_player_count = 0

service = None

scheduler = sched.scheduler(time.time, time.sleep)

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
        if len(all_players):    
            pass
            players_db.insert_many(all_players, ordered=False) # ordered=False to soft ignore duplicates
    except pymongo.errors.BulkWriteError:
        pass # dont care, didnt ask

def write_to_sheets():
    global previous_time, old_player_count
    now = datetime.now()
    if previous_time == int(now.strftime("%H")) - 1: # every hour insert player count and time
        player_count = players_db.estimated_document_count()
        insert([now.strftime("%Y/%m/%d - %H:%M:%S"), player_count - old_player_count])
        old_player_count = player_count
    elif previous_time == 23 and int(now.strftime("%H")) == 0: # on day end, insert the current days count and clear collection
        player_count = players_db.estimated_document_count()
        insert([now.strftime("%Y/%m/%d - %H:%M:%S"), player_count - old_player_count]) # enters hourly data
        insert([now.strftime("%Y/%m/%d - %H:%M:%S"), players_db.estimated_document_count()], True) # enters daily data
        players_db.delete_many({}) # deletes all data in collection
        old_player_count = 0
    previous_time = int(now.strftime("%H"))  

def get_fights_data(sc, counter): 
    for i in range(0, 250, 50):
        try:
            data = session.get(f"https://gameinfo.albiononline.com/api/gameinfo/events?limit=50&offset={i}").result()
        except:
            print("Request timed out")
        if data.status_code != 200:
            print("error: ", data.status_code)
            continue
        get_participants(data)
        
    print(counter)
    counter += 1
    write_to_sheets()
    scheduler.enter(60, 1, get_fights_data, (sc, counter, ))

def main():
    scheduler.enter(60, 1, get_fights_data, (scheduler, counter, ))
    scheduler.run()

def init_sheets():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials(enable_reauth_refresh=True)
        creds = creds.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    service = build('sheets', 'v4', credentials=creds)
    return service;

def get_length(service, end_of_day=False):
    sheet = service.spreadsheets()
    data_range = "A:A"
    if end_of_day:
        data_range = "D:D"
    result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                                range=data_range).execute()
    values = result.get('values', [])
    return len(values)
    
def insert(values, end_of_day=False):
    global service
    if service == None:
        service = init_sheets()
        
    values = [values]
    body = {
        'values': values
    }
    position = get_length(service, end_of_day) + 1
    
    pos_1 = "A" # cell position
    pos_2 = "B"
    if end_of_day:
        pos_1 = "D"
        pos_2 = "E"

    result = service.spreadsheets().values().update(
        spreadsheetId=SAMPLE_SPREADSHEET_ID, range=f"{pos_1}{position}:{pos_2}{position}",
        valueInputOption="USER_ENTERED", body=body).execute()
    print('{0} cells updated.'.format(result.get('updatedCells')))


if __name__ == '__main__':
    main()

