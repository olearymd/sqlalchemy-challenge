# 
# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, inspect
from flask import Flask, jsonify
import numpy as np
import pandas as pd
import datetime as dt

####
# Flask Setup
####
app = Flask(__name__)

####
# Flask Routes
####
@app.route("/")
def welcome():
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/tobs_by_date/&lt;start&gt; <br/>"
        f"/api/v1.0/tobs_by_date/&lt;start&gt;/&lt;end&gt;<br/>"
    )

####
# SQL Alchemy ORM initilization
####
# Create engine to hawaii.sqlite
engine = create_engine("sqlite:///Resources/hawaii.sqlite")
# 
# Reflect an existing database into a new model
Base = automap_base()
# 
# Reflect the tables
Base.prepare(autoload_with=engine)
# 
# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station


####
# /api/v1.0/precipitation
####  
@app.route("/api/v1.0/precipitation")
def precipitation():

    # Create session from Python to the DB:
    session = Session(engine)

    # Get the date and precipitation for the last 12 months of data in the data set:
    # 
    # Returns a list of tuples, like this:
    # [('2016-08-24', 0.08), ('2016-08-24', 2.15), ('2016-08-24', 2.28), ...
    precip = session.query(Measurement.date, Measurement.prcp).\
        filter(
            Measurement.date > (
                # Subquery to get the most recent date in the table and subtrack 365 days 
                session.query(func.date(func.datetime(Measurement.date, '-365 days')))\
                .order_by(Measurement.date.desc()).limit(1).scalar()
            ) 
        )\
        .order_by(Measurement.date).all()

    # Close the session as soon as it's no longer needed:
    session.close()

    # Convert list of tuples to a dictionary:
    precip_dict = {}
    precip_dict = dict(precip)
    # print(precip_dict)

    # Return the JSON representation of your dictionary.
    return jsonify(precip_dict)

####
# /api/v1.0/stations
####
# 
# Return a JSON list of stations from the dataset
#
@app.route("/api/v1.0/stations")
def stations():
    # Create session from Python to the DB:
    session = Session(engine)

    # Run query to get list of stations:
    results = session.query(Station.station, Station.name, Station.latitude, Station.longitude, Station.elevation).all()
    print(f"results = {results}")

    # Close the session as soon as it's no longer needed:
    session.close()

    # Create a dictionary from the row data and append to a list of all_passengers
    station_list = []
    for station, name, latitude, longitude, elevation in results:
        station_dict = {}
        station_dict["station"] = station
        station_dict["name"] = name
        station_dict["latitude"] = latitude
        station_dict["longitude"] = longitude
        station_dict["elevation"] = elevation
        station_list.append(station_dict)
    print(f"station_dict = {station_dict}")
    print(f"station_list = {station_list}")

    # Return the json list of dictionaries:
    return jsonify(station_list)

#### 
# /api/v1.0/tobs
#### 
@app.route("/api/v1.0/tobs")
def tobs():
    # Create session from Python to the DB:
    session = Session(engine) 

    # Query the dates and temperature observations of the most-active station for the previous 
    # year of data.
    
    # This query gets us the top station:
    top_station_all = session.query(Measurement.station, Station.name, func.count(Measurement.tobs))\
        .join(Station, Station.station == Measurement.station)\
        .group_by(Measurement.station)\
        .order_by(func.count(Measurement.tobs).desc()).limit(1).all()
    top_station = top_station_all[0][0]
    print(f"top_station = {top_station}")

    # This query gets us the previous year starting date:
    previous_year = session.query(func.date(func.datetime(Measurement.date, '-365 days')))\
        .order_by(Measurement.date.desc()).limit(1).scalar()
    print(f"previous_year = {previous_year}")

    # This query gets us the previous year's temperature observations from the top station:
    temps = session.query(Measurement.date, Measurement.tobs)\
        .filter(Measurement.date > (previous_year))\
        .filter(Measurement.station == top_station)\
        .order_by(Measurement.date).all()
    # print(f"temps = {temps}")
    # temps is a list of tuples:
    # [('2016-08-24', 77.0), ('2016-08-25', 80.0), ... 

    # Close the session as soon as it's no longer needed:
    session.close() 

    # Create a dictionary from the row data and append to a list of for the temperature data.
    temps_list = []
    for date, temp in temps:
        temps_dict = {}
        temps_dict["Date"] = date
        temps_dict["Temperature"] = temp
        temps_list.append(temps_dict)
    # print(f"temps_dict = {temps_dict}")
    # print(f"temps_list = {temps_list}") 

    return jsonify(temps_list)

####
# /api/v1.0/tobs_by_date/<start>
####
@app.route("/api/v1.0/tobs_by_date/<start>")
def tobs_by_date(start):
    # Create session from Python to the DB:
    session = Session(engine) 

    # Return a JSON list of the minimum temperature, the average temperature, and the maximum 
    # temperature for a specified start or start-end range.
    # 
    # For a specified start, calculate TMIN, TAVG, and TMAX for all the dates greater than or 
    # equal to the start date.
    # 
    measurements = session.query(
        func.min(Measurement.tobs), 
        func.max(Measurement.tobs), 
        func.avg(Measurement.tobs))\
        .filter(Measurement.date >= (start)).all()
    print(f"measurements = {measurements}")

    # Close the session as soon as it's no longer needed:
    session.close() 

    # Create a dictionary from the measurement data and append to a list dictionaries for jsonify
    obs_list = []
    for min, max, avg in measurements:
        obs_dict = {}
        obs_dict["Min Temp"] = min
        obs_dict["Max Temp"] = max
        obs_dict["Average Temp"] = avg
        obs_list.append(obs_dict)

    # Return a JSON list of temperature observations for the previous year.
    # 
    return jsonify(obs_list) 

####
# /api/v1.0/tobs_by_date/<start>/<end>
####
@app.route("/api/v1.0/tobs_by_date/<start>/<end>")
def tobs_by_date_start_end(start, end):
    # Create session from Python to the DB:
    session = Session(engine) 

    # Return a JSON list of the minimum temperature, the average temperature, and the maximum 
    # temperature for a specified start or start-end range.
    # 
    # This query calculates the min, max, and average temperature for the date range specified 
    measurements = session.query(
        func.min(Measurement.tobs),
        func.max(Measurement.tobs),
        func.avg(Measurement.tobs))\
        .filter(Measurement.date >= (start))\
        .filter(Measurement.date <= (end)).all()
    # print(measurements)

    # Close the session as soon as it's no longer needed:
    session.close() 

    # Create a dictionary from the measurement data and append to a list dictionaries for jsonify
    obs_list = []
    for min, max, avg in measurements:
        obs_dict = {}
        obs_dict["Min Temp"] = min
        obs_dict["Max Temp"] = max
        obs_dict["Average Temp"] = avg
        obs_list.append(obs_dict)

    # Return a JSON list of temperature observations for the previous year.
    # 
    return jsonify(obs_list) 

if __name__ == '__main__':
    app.run(debug=True)