#imports
import http.client
import requests
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

#loading dot env variables
load_dotenv()
RAPID_API_KEY = os.getenv("RAPID_API_KEY")
DATABASE = os.getenv("DATABASE")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")


def get_data():
  # API config
  # API for NBA Games results

  conn = http.client.HTTPSConnection("api-nba-v1.p.rapidapi.com")

  headers = {
      'X-RapidAPI-Key': RAPID_API_KEY,
      'X-RapidAPI-Host': "api-nba-v1.p.rapidapi.com"
  }

  """
  Getting specific date requests
  delta days to most played dates
  """
  yesterday_date = datetime.now() - timedelta(days=730)
  yesterday_date_str = yesterday_date.strftime("%Y-%m-%d")


  conn.request("GET", f"/games?date={yesterday_date_str}", headers=headers) 
  res = conn.getresponse()
  data = res.read()
  print(data.decode("utf-8"))


  querystring = {"date":yesterday_date_str}
  url = "https://api-nba-v1.p.rapidapi.com/games"

  # request to API
  response = requests.get(url, headers=headers, params=querystring)

  # convert json data into python dictionary
  datos_json = response.json()
  # print (datos_json)
  
def connect_database():
  # driver implementation - using psycog2

  try:
    conn = psycopg2.connect(database=DATABASE, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port = DB_PORT)
    print("conexion exitosa")
  except Exception as e:
    print("conexion fallida")
    print(f"Error: {e}")
    
  
def create_table():
  #Create table
  createTableGames = """
      CREATE TABLE IF NOT EXISTS cmlocastro20_coderhouse.nba_games (
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

  # Create table in DB

  # using a cursor to run SQL commands
  cursor = conn.cursor()
  cursor.execute(createTableGames)
  conn.commit()

  #truncate table 
  #cursor.execute("Truncate table cmlocastro20_coderhouse.nba_games")
  #conn.commit()

def insert_data():
  # convert data into dataframe to easy handle
  df = pd.DataFrame(datos_json['response'])

  # drop duplicados
  #df.drop_duplicates(inplace=True)

  # fill null values with default values if apply
  df.fillna(0, inplace=True)

  # preparing data to insert
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

  # define query to insert
  insert_query = """
      INSERT INTO cmlocastro20_coderhouse.nba_games VALUES %s;
  """
  
  # execute insert
  execute_values(cursor, insert_query, data_to_insert)
  conn.commit()
  
  
def disconnect_database():
  #Dissconect db
  try:
    cursor.close()
    conn.close()
    print("db desconectada")
  except Exception as e:
    print("error en desconexion")
    print(f"Error: {e}")

