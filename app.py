#################################################
# Dependencies
#################################################
# Flask (Server)
from flask import Flask, jsonify, render_template, request, flash, redirect
from flask_pymongo import PyMongo

# SQL Alchemy (ORM)
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, desc,select

import pandas as pd
import numpy as np



#################################################
# Flask Setup
#################################################
app = Flask(__name__)

# Use flask_pymongo to set up mongo connection
app.config["MONGO_URI"] = "mongodb://localhost:27017/nfl"
mongo = PyMongo(app)
#################################################
# Flask Routes
#################################################
# Returns the dashboard homepage
@app.route("/")
@app.route('/<path>')
def index(path):
    content   = None 
    dashboard = None 

    if path:
        content = render_template( '/' + path + '.html')
    else:
        content   = render_template( '/dashboard.html' )
        
    if not path or path == 'dashboard':
        dashboard = True

    return render_template( '/base.html',
                            content   = content,
                            path      = path,
                            dashboard = dashboard ) 
    #return render_template("index.html")



################################################
# Returns a json dictionary of player information
@app.route('/players/<playername>')
def get_player_by_name(playername):
    query={"name":{ "$regex": "^"+playername }}
    print(playername)
    #result=mongo.db.profiles.find_one(query)
    exclude_data = {'_id': False}
    result=mongo.db.profiles.find_one(query,projection=exclude_data)
    print(result)

    return jsonify(result)

@app.route('/api/performance/<playername>')
def get_performance_by_name(playername):
    query={"name":{ "$regex": "^"+playername }}
    print(playername)
    #result=mongo.db.profiles.find_one(query)
    exclude_data = {'_id': False}
    result=mongo.db.profiles.find_one(query,projection=exclude_data)
    print(result)

    query = {"player_id":result["player_id"]}
    
    raw_data = list(mongo.db.games.find(query, projection=exclude_data))
    df = pd.DataFrame(raw_data)
    groupbydata=df.groupby("year")
    summary_df=pd.DataFrame({"total_passing_yards":groupbydata['passing_yards'].sum()})
    summary_df.head()
    summary_df=summary_df.reset_index()

    trace = {
        "x": summary_df["year"].values.tolist(),
        "y": summary_df["total_passing_yards"].values.tolist(),
        "type": "bar"
    }

    return jsonify(trace)

@app.route('/api/teamperformance/<playername>')
def get_team_performance_by_name(playername):  
    exclude_data = {'_id': False}
    query={"name":{ "$regex": "^"+playername }}
    result=mongo.db.profiles.find_one(query,projection=exclude_data)
    print(result)
    query = {"player_id":result["player_id"],"game_won":True}
    
    raw_data = list(mongo.db.games.find(query, projection=exclude_data))
    df = pd.DataFrame(raw_data)
    groupbydata=df.groupby("year")
    summary_df=pd.DataFrame({"wins":groupbydata['age'].count()})
    summary_df=summary_df.reset_index()

    trace = {
        "x": summary_df["year"].values.tolist(),
        "y": summary_df["wins"].values.tolist(),
        "mode": "markers",
        "marker":{
            "size":summary_df["wins"].values.tolist()
        }
    }

    return jsonify(trace)

@app.route('/api/playerprofiles')
def get_player_profiles():
    playername = request.args.get('player_name')
    exclude_data = {'_id': False}

    print(playername)
    query={}
    if(playername!=None):
        query={"name":{ "$regex": "^"+playername },"position":"QB"}
    print("querying")
    results=mongo.db.profiles.find(query,projection=exclude_data)
    profiles_list=[]
    for r in results:
        r["player_name"]="<a href='/dashboard?playername="+r["name"]+"'>"+r['name']+"</a>"
        profiles_list.append(r)
    return jsonify(profiles_list)

    

@app.route('/api/playerwinningpercentage/<player_name>')
def get_player_winning_percecntage(player_name):
    exclude_data = {'_id': False}
    query={"name":{ "$regex": "^"+player_name }}
    result=mongo.db.profiles.find_one(query,projection=exclude_data)
    query = {"player_id":result["player_id"]} 
    raw_data = list(mongo.db.games.find(query, projection=exclude_data))
    df = pd.DataFrame(raw_data)
    total=len(df.index)
    
    away_df=df.loc[df['game_location']=='A'].loc[df['game_won']==True]
    away_wins=len(away_df.index)

    home_df=df.loc[df['game_location']=='H'].loc[df['game_won']==True]
    len(home_df)
    home_wins=len(home_df.index)

    percentages = {
        "home":(home_wins/total)*100,
        "away":(away_wins/total)*100,
    }

    return jsonify(percentages)



if __name__ == "__main__":
    app.run(debug=True)
   
    