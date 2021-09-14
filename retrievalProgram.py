import json
import requests
import mysql.connector
import datetime
import sys

LOCATIONS = {"Cairo": 1521894, "Athens": 946738, "Johannesburg": 1582504}
DATE_STR = "2021/2/1"
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="metaweather"
)


def get_response(date):
    forecasts = []
    for city, woeid in LOCATIONS.items():
        resp = requests.get("https://www.metaweather.com/api/location/" + str(woeid) + "/" + date)
        tmp_forecasts = json.loads(resp.text)
        seven_period_forecasts = []
        for tmp in tmp_forecasts:
            date_object = datetime.datetime.strptime(tmp['created'], "%Y-%m-%dT%H:%M:%S.%f%z")
            tmp['created'] = date_object.strftime('%Y-%m-%d %H:%M:%S')
            tmp['woeid'] = woeid
            tmp['city_name'] = city
            if (datetime.datetime.strptime(DATE_STR, '%Y/%m/%d').date() - date_object.date()).days < 7:
                seven_period_forecasts.append(tmp)
        forecasts.extend(seven_period_forecasts)
        print("HTTP requests sent: https://www.metaweather.com/api/location/" + str(woeid) + "/" + DATE_STR)
    return forecasts



def insert_to_database(mdb, data):
    cursor = mdb.cursor()
    sql = "INSERT INTO `forecasts` (`id`, `weather_state_name`, `wind_direction_compass`, `created`, " \
          "`applicable_date`, `min_temp`, `max_temp`, `the_temp`, `wind_speed`, `wind_direction`, `air_pressure`, " \
          "`humidity`, `visibility`, `predictability`, `woeid`, `city_name`) VALUES (" \
          "%(id)s, %(weather_state_name)s, %(wind_direction_compass)s, %(created)s, %(applicable_date)s, " \
          "%(min_temp)s, %(max_temp)s, %(the_temp)s, %(wind_speed)s, %(wind_direction)s, %(air_pressure)s, " \
          "%(humidity)s, %(visibility)s, %(predictability)s, %(woeid)s, %(city_name)s) "
    cursor.executemany(sql, data)
    mdb.commit()


DATE_STR = input("Enter a date in format (Y/M/D): ") 
data = get_response(DATE_STR)
insert_to_database(mydb, data)

