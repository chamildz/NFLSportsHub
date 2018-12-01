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
# Database Setup
#################################################
engine = create_engine("sqlite:///DataSets/belly_button_biodiversity.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
OTU = Base.classes.otu
Samples = Base.classes.samples
Samples_Metadata= Base.classes.samples_metadata

# Create our session (link) from Python to the DB
session = Session(engine)

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

#################################################
# Returns a list of sample names
@app.route('/names')
def names():
    """Return a list of sample names."""
    session = Session(engine)

    # Use Pandas to perform the sql query
    stmt = session.query(Samples).statement
    df = pd.read_sql_query(stmt, session.bind)
    df.set_index('otu_id', inplace=True)

    # Return a list of the column names (sample names)
    return jsonify(list(df.columns))

#################################################
# Returns a list of OTU descriptions 
@app.route('/otu')
def otu():
    """Return a list of OTU descriptions."""
    session = Session(engine)
    results = session.query(OTU.lowest_taxonomic_unit_found).all()

    # Use numpy ravel to extract list of tuples into a list of OTU descriptions
    otu_list = list(np.ravel(results))
    return jsonify(otu_list)

#################################################
# Returns a json dictionary of sample metadata 
@app.route('/metadata/<sample>')
def sample_metadata(sample):
    """Return the MetaData for a given sample."""
    sel = [Samples_Metadata.SAMPLEID, Samples_Metadata.ETHNICITY,
           Samples_Metadata.GENDER, Samples_Metadata.AGE,
           Samples_Metadata.LOCATION, Samples_Metadata.BBTYPE]

    # sample[3:] strips the `BB_` prefix from the sample name to match
    # the numeric value of `SAMPLEID` from the database
    session = Session(engine)
    results = session.query(*sel).\
        filter(Samples_Metadata.SAMPLEID == sample[3:]).all()

    # Create a dictionary entry for each row of metadata information
    sample_metadata = {}
    for result in results:
        sample_metadata['SAMPLEID'] = result[0]
        sample_metadata['ETHNICITY'] = result[1]
        sample_metadata['GENDER'] = result[2]
        sample_metadata['AGE'] = result[3]
        sample_metadata['LOCATION'] = result[4]
        sample_metadata['BBTYPE'] = result[5]

    return jsonify(sample_metadata)

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
    playername = request.args.get('name')
    exclude_data = {'_id': False}

    print(playername)
    query={}
    if(playername!=None):
        query={"name":{ "$regex": "^"+playername }}
    print("querying")
    results=mongo.db.profiles.find(query,projection=exclude_data)
    profiles_list=[]
    for r in results:
        r["player_name"]="<a href='/dashboard?playername="+r["name"]+"'>"+r['name']+"</a>"
        profiles_list.append(r)
    return jsonify(profiles_list)


#################################################
# Returns an integer value for the weekly washing frequency `WFREQ`
@app.route('/wfreq/<sample>')
def sample_wfreq(sample):
    """Return the Weekly Washing Frequency as a number."""

    # `sample[3:]` strips the `BB_` prefix
    session = Session(engine)
    results = session.query(Samples_Metadata.WFREQ).\
        filter(Samples_Metadata.SAMPLEID == sample[3:]).all()
    wfreq = np.ravel(results)

    # Return only the first integer value for washing frequency
    return jsonify(int(wfreq[0]))

#################################################
# Return a list of dictionaries containing sorted lists  for `otu_ids`and `sample_values`
@app.route('/samples/<sample>')
def samples(sample):
    """Return a list dictionaries containing `otu_ids` and `sample_values`."""
    session = Session(engine)
    stmt = session.query(Samples).statement
    df = pd.read_sql_query(stmt, session.bind)

    # Make sure that the sample was found in the columns, else throw an error
    if sample not in df.columns:
        return jsonify(f"Error! Sample: {sample} Not Found!"), 400

    # Return any sample values greater than 1
    df = df[df[sample] > 1]

    # Sort the results by sample in descending order
    df = df.sort_values(by=sample, ascending=0)

    # Format the data to send as json
    data = [{
        "otu_ids": df[sample].index.values.tolist(),
        "sample_values": df[sample].values.tolist()
    }]
    return jsonify(data)
if __name__ == "__main__":
    app.run(debug=True)
   
    