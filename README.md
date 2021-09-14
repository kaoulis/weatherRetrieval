# weatherRetrieval
Custom API for weather forecast KPIs

OBJECTIVES
1.	Create and store the data from https://www.metaweather.com/api/ in a relational database.
2.	Create an api that uses the downloaded data and provides endpoints.


DOCUMENTATION
1.	Run create_database.sql
2.	Open the .py files with a notebook and change the database credentials.
3.	Run retrievalProgram.py
4.	Run api.py

You can check the api methods in localhost http://127.0.0.1:5002/.
The available endpoints are:
1.	/locations (List locations)
2.	/latest_daily_forecasts (List the average the_temp of the last 3 forecasts for each location for every day)
3.	/latest_daily_forecasts_byday (List the average the_temp of the last 3 forecasts for each location for every day - modified)
4.	/avg_temp (List the average the_temp of the last 3 forecasts for each location for every day)
5.	/top/<n> (Get the top n locations based on each available metric where n is a parameter given to the api call.)
