# -*- coding: utf-8 -*-
"""Primera Entrega.ipynb

# Primera Entrega
## Realizar solicitud a API
"""

# installing requited packages
!pip install requests
!pip install psycopg2

# import required library

import requests
import http.client
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import pandas as pd

"""
Script commented: get data from specific date
"""

#TODO: get data from yesterday (not today) due there are games which aren unfinished yet


# from datetime import datetime
# fecha_hoy = datetime.now().strftime("%Y-%m-%d")
# querystring = {"date": fecha_hoy}
# print (querystring)



# API Config

conn = http.client.HTTPSConnection("api-nba-v1.p.rapidapi.com")
# X-RapidAPI-Key is not encripted to make easy being runned during approval
headers = {
    'X-RapidAPI-Key': "b558266288mshd5821604c5106d8p10a619jsnd923a5718446",
    'X-RapidAPI-Host': "api-nba-v1.p.rapidapi.com"
}

"""
Query to an specific game to get many results as possible
"""
conn.request("GET", "/games?date=2022-02-12", headers=headers)
res = conn.getresponse()
data = res.read()

print(data.decode("utf-8"))

#API about NBA
#Endpoint games: Games results for specific day

querystring = {"date":"2022-02-12"}
url = "https://api-nba-v1.p.rapidapi.com/games"
response = requests.get(url, headers=headers, params=querystring)

# Convert JSON data to a Python dictionary
datos_json = response.json()
print (datos_json)

# Database connection

#creation connection variables
from google.colab import userdata

db = "data-engineer-database"
user = "cmlocastro20_coderhouse"
password = userdata.get('password')
host = "data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
port = "5439"

#Implementation - using psycog2 -Test
import psycopg2
try:
  conn = psycopg2.connect(database=db, user=user, password=password, host=host, port = port)
  print("conexion exitosa")
except Exception as e:
  print("conexion fallida")
  print(f"Error: {e}")

#Create table
createTableGames = """
    CREATE TABLE IF NOT EXISTS nba_games (
        id INT PRIMARY KEY,
        league VARCHAR(255),
        season INT,
        start_date TIMESTAMP,
        end_date TIMESTAMP,
        duration VARCHAR(255),
        stage INT,
        status_short INT,
        status_long VARCHAR(255),
        current_periods INT,
        total_periods INT,
        end_of_period BOOLEAN,
        arena_name VARCHAR(255),
        arena_city VARCHAR(255),
        arena_state VARCHAR(255),
        arena_country VARCHAR(255),
        visitors_team_name VARCHAR(255),
        visitors_team_logo VARCHAR(255),
        home_team_name VARCHAR(255),
        home_team_logo VARCHAR(255),
        visitors_win INT,
        visitors_loss INT,
        visitors_series_win INT,
        visitors_series_loss INT,
        visitors_linescore VARCHAR(255),
        visitors_points INT,
        home_win INT,
        home_loss INT,
        home_series_win INT,
        home_series_loss INT,
        home_linescore VARCHAR(255),
        home_points INT,
        officials VARCHAR(255),
        times_tied INT,
        lead_changes INT
    );
    """
# duration INT, should be interval
# visitors_linescore VARCHAR(255)[] should be an array, its not supported by redshift
# home_linescore VARCHAR(255)[] should be an array, its not supported by redshift
# officials VARCHAR(255)[] should be an array, its not supported by redshift

# create table in DB

# Create cursor to execute SQL commands
cursor = conn.cursor()
cursor.execute(createTableGames)
conn.commit()

#truncate table -
cursor.execute("Truncate table nba_games")
conn.commit()

from psycopg2.extras import execute_values

# Data insert

data_to_insert = []
for game in datos_json['response']:
    data_to_insert.append((
        game['id'],
        game['league'],
        game['season'],
        game['date']['start'],
        game['date']['end'],
        game['date']['duration'],
        game['stage'],
        game['status']['short'],
        game['status']['long'],
        game['periods']['current'],
        game['periods']['total'],
        bool(game['periods']['endOfPeriod']),
        game['arena']['name'],
        game['arena']['city'],
        game['arena']['state'],
        game['arena']['country'],
        game['teams']['visitors']['name'],
        game['teams']['visitors']['logo'],
        game['teams']['home']['name'],
        game['teams']['home']['logo'],
        game['scores']['visitors']['win'],
        game['scores']['visitors']['loss'],
        game['scores']['visitors']['series']['win'],
        game['scores']['visitors']['series']['loss'],
        str(game['scores']['visitors']['linescore']),
        game['scores']['visitors']['points'],
        game['scores']['home']['win'],
        game['scores']['home']['loss'],
        game['scores']['home']['series']['win'],
        game['scores']['home']['series']['loss'],
        str(game['scores']['home']['linescore']),
        game['scores']['home']['points'],
        str(game['officials']),
        game['timesTied'],
        game['leadChanges']
    ))

# Insert Query
insert_query = """
    INSERT INTO nba_games VALUES %s;
"""

# execute insert
execute_values(cursor, insert_query, data_to_insert)


# commit changes in DB
conn.commit()

#disconnect DB
try:
  cursor.close()
  conn.close()
  print("db desconectada")
except Exception as e:
  print("error en desconexion")
  print(f"Error: {e}")

