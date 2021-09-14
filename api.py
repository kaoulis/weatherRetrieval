import mysql.connector
import pandas as pd
import json
from flask import request, jsonify, Flask
from flask_restful import Resource, Api, reqparse

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="metaweather"
)
cursor = mydb.cursor(buffered=True)
app = Flask(__name__)
api = Api(app)


class Locations(Resource):
    def get(self):
        cursor.execute("select distinct city_name from forecasts;")
        result = cursor.fetchall()
        return [i[0] for i in result]


class LatestDailyForecasts(Resource):
    def get(self):
        cursor.execute("SELECT * FROM ( " \
                       "SELECT *, RANK() OVER ( " \
                       "PARTITION BY YEAR(created), MONTH(created), DAY(created), city_name " \
                       "ORDER BY created DESC) AS forecasts_rank " \
                       "FROM forecasts " \
                       ") AS ranked_table " \
                       "WHERE forecasts_rank = 1;")
        result = [dict(zip(tuple(cursor.column_names), i)) for i in cursor.fetchall()]
        print(result)
        return jsonify(result)


class LatestDailyForecastsBYDAY(Resource):
    def get(self):
        cursor.execute("SELECT * FROM ( " \
                       "SELECT *, RANK() OVER ( " \
                       "PARTITION BY YEAR(created), MONTH(created), DAY(created), city_name " \
                       "ORDER BY created DESC) AS forecasts_rank " \
                       "FROM forecasts " \
                       ") AS ranked_table " \
                       "WHERE forecasts_rank = 1;")
        df = pd.DataFrame(cursor.fetchall(),
                          columns=['id', 'woeid', 'city_name', 'weather_state_name', 'wind_direction_compass',
                                   'created', 'applicable_date', 'min_temp', 'max_temp', 'the_temp', 'wind_speed',
                                   'wind_direction', 'air_pressure', 'humidity', 'visibility', 'predictability',
                                   'forecasts_rank'])

        df['date'] = df['created'].dt.date
        del df['forecasts_rank']

        # test = df.groupby('date').apply(lambda x: x.to_dict(orient='records'))
        # print(test.to_dict())
        j = (df.groupby(['date'])
             .apply(lambda x: x.to_dict('r'))
             .reset_index()
             .rename(columns={0: 'Forecast-Data'})
             .to_json(orient='records'))
        print(j)
        return json.loads(j)


class AvgTemp(Resource):
    def get(self):
        cursor.execute("SELECT created, city_name, avg(the_temp) AS avg_temp FROM ( " \
                       "SELECT *, RANK() OVER ( " \
                       "PARTITION BY YEAR(created), MONTH(created), DAY(created), city_name " \
                       "ORDER BY created DESC) AS forecasts_rank " \
                       "FROM forecasts " \
                       ") AS ranked_table " \
                       "WHERE forecasts_rank <= 3 GROUP BY YEAR(created), MONTH(created), DAY(created), city_name;")
        result = [dict(zip(tuple(cursor.column_names), i)) for i in cursor.fetchall()]
        print(result)
        return jsonify(result)


class TopN(Resource):
    def get(self, n):
        # parser = reqparse.RequestParser()  # initialize
        # parser.add_argument('n', required=True)
        # args = parser.parse_args()  # parse arguments to dictionary
        cursor.execute("SELECT city_name, avg(min_temp), avg(max_temp), avg(wind_speed), "
                       "avg(air_pressure), avg(humidity), avg(visibility), avg(predictability) FROM forecasts GROUP "
                       "BY city_name;")
        df = pd.DataFrame(cursor.fetchall(),
                          columns=['city_name', 'avg(min_temp)', 'avg(max_temp)', 'avg(wind_speed)',
                                   'avg(air_pressure)', 'avg(humidity)', 'avg(visibility)', 'avg(predictability)'])
        result = [{'Top min_temp': json.loads((df.sort_values('avg(min_temp)', ascending=True).head(int(n)))[['city_name', 'avg(min_temp)']].to_json(orient='records'))},
                  {'Top max_temp': json.loads((df.sort_values('avg(max_temp)', ascending=False).head(int(n)))[
                                                  ['city_name', 'avg(max_temp)']].to_json(orient='records'))},
                  {'Top wind_speed': json.loads((df.sort_values('avg(wind_speed)', ascending=False).head(int(n)))[
                                                    ['city_name', 'avg(wind_speed)']].to_json(orient='records'))},
                  {'Top air_pressure': json.loads((df.sort_values('avg(air_pressure)', ascending=False).head(int(n)))[
                                                      ['city_name', 'avg(air_pressure)']].to_json(orient='records'))},
                  {'Top humidity': json.loads((df.sort_values('avg(humidity)', ascending=False).head(int(n)))[
                                                  ['city_name', 'avg(humidity)']].to_json(orient='records'))},
                  {'Top visibility': json.loads((df.sort_values('avg(visibility)', ascending=False).head(int(n)))[
                                                    ['city_name', 'avg(visibility)']].to_json(orient='records'))},
                  ]
        return result


api.add_resource(Locations, '/locations')  # Route_1
api.add_resource(LatestDailyForecasts, '/latest_daily_forecasts')  # Route_2
api.add_resource(LatestDailyForecastsBYDAY, '/latest_daily_forecasts_byday')  # Route_2.1
api.add_resource(AvgTemp, '/avg_temp')  # Route_3
api.add_resource(TopN, '/top/<n>')  # Route_4

if __name__ == '__main__':
    app.run(port='5002')