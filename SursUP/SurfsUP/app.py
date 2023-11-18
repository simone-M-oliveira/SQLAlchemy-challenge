# Import the dependencies.
import datetime as dt

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify


#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(autoload_with=engine)

# Save references to each table
Station = Base.classes.station
Measurement = Base.classes.measurement

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)



#################################################
# Flask Routes
#################################################
@app.route("/")
def home():
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/&lt;start&gt;<br/>"
        f"/api/v1.0/&lt;start&gt;/&lt;end&gt;<br/>"
    )

@app.route("/api/v1.0/precipitation")
def get_precipitation_data():

    # Get the latest observation date
    max_date = session.query(func.max(Measurement.date)).first()

    # Convert it to a datetime object
    end_date = dt.datetime.strptime(max_date[0], '%Y-%m-%d')

    # Calculate a year in the past
    start_date = end_date - dt.timedelta(days=366)

    # Select date and precipitation (prcp) from the measurement table where where the observation date is in the last year 
    precip_results = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date.between(start_date, end_date)).order_by(Measurement.date).all()

    # For each result, add the date as the key and precipitation as the value
    all_measurements = []
    for date, prcp in precip_results:
        measurement_dict = {}
        measurement_dict[date] = prcp
        all_measurements.append(measurement_dict)
    
    # Return a list of date/precipitation key/value pairs as json
    return jsonify(all_measurements)

@app.route("/api/v1.0/stations")
def get_stations():
    
    # Select all fields from the station table
    station_results = session.query(Station.station, Station.name, Station.latitude, Station.longitude, Station.elevation).all()

    # For each result, add the fields a dictionary object and append to the results list
    all_stations = []
    for station, name, latitude, longitude, elevation in station_results:
        station_dict = {}
        station_dict['station'] = station
        station_dict['name'] = name
        station_dict['latitude'] = latitude
        station_dict['longitude'] = longitude
        station_dict['elevation'] = elevation
        all_stations.append(station_dict)
    
    # Return a list of station dictionary objects as json
    return jsonify(all_stations)

@app.route("/api/v1.0/tobs")
def get_temp_observations():

    # Get the station that has had the most measurements (ie - most active)
    most_active_station_id = session.query(Measurement.station).group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).first()
    
    # Retrieve the latest observation date and covert it to a datetime object
    max_date = session.query(func.max(Measurement.date)).filter(Measurement.station == most_active_station_id[0]).group_by(Measurement.station).first()
    end_date = dt.datetime.strptime(max_date[0], '%Y-%m-%d')

    # Calculate a year in the past
    start_date = end_date - dt.timedelta(days=366)
    
    # Select the date and temperature (tobs) for the most active station where the observation date is in the past year
    temperature_results = session.query(Measurement.date, Measurement.tobs).filter(Measurement.station == most_active_station_id[0], Measurement.date.between(start_date, end_date))

    # For each result, add the date and temperature to a dictionary and append the dictionary object to a list
    all_temperatures = []
    for date, tobs in temperature_results:
        temperature_dict = {}
        temperature_dict['date'] = date
        temperature_dict['tobs'] = tobs
        all_temperatures.append(temperature_dict)

    # Return a list of date/temperature objects as json
    return jsonify(all_temperatures)

@app.route("/api/v1.0/<start>/<end>")
def get_temp_statistics_range(start, end):

    # Create the base query
    # Select date and min, avg, and max temperature (tobs) where the observation date is greater than or equal to the provided date
    base_query = session.query(Measurement.date, func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).filter(Measurement.date >= start)

    # If no end date is provided, use that base query
    # If an end date is provided, append the end date filter to the existing query object
    if (end is None):
        observation_results = base_query
    else:
        observation_results = base_query.filter(Measurement.date <= end)
    
    # Execute the crafted query, grouped by the date
    observation_results_by_date = observation_results.group_by(Measurement.date)

    # For each result, create a dictionary object that stores the date, min, avg, and max temperature and append it to the overall list
    all_observations = []
    for date, min, avg, max in observation_results_by_date:
        observation_dict = {}
        observation_dict['Date'] = date
        observation_dict['TMIN'] = min
        observation_dict['TAVG'] = avg
        observation_dict['TMAX'] = max
        all_observations.append(observation_dict)

    # Return the list of observations as json
    return jsonify(all_observations)
    
@app.route("/api/v1.0/<start>")
def get_temp_statistics_start(start):

    # When only receiving the start date, execute get_temp_statistics_range with no end date on the range
    # get_temp_statistics_range returns json
    return get_temp_statistics_range(start, None)

if __name__ == '__main__':
    app.run(debug=True)