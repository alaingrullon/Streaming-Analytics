import socket
import sys
import requests
import json
from pprint import pprint
import numpy as np
from datetime import *
import csv
from csv import DictReader
from time import sleep

TCP_IP = "192.168.1.25"
TCP_PORT = 9091

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)

def average(lst): 
    return sum(lst) / len(lst)


while True:
    with open('sdq_weather_forecasts.csv') as read_obj:
        csv_to_dict = csv.DictReader(read_obj)

        print("Waiting for TCP connection...") 
        conn, addr = s.accept()
        print("Connected... Starting getting weather.")

        i = 1
        irradiance_list = []
        irradiance_cm_avg = 0

        for line in csv_to_dict: 
            # Extracting all features
            time = line['DateTime']
            temp = float(line['temp'])
            ws = float(line['wind_speed'])
            pressure = float(line['pressure'])
            humidity = float(line['humidity'])
            rain = float(line['rain'])
            hr_sin = float(line['hr_sin'])
            hr_cos = float(line['hr_cos'])
            mnth_sin = float(line['mnth_sin'])
            mnth_cos = float(line['mnth_cos'])

            # Prediction with multivariate linear regression formula
            irradiance = round(-5649.96 + 60.16*temp + 15.05*ws + 4.24*pressure + 
                               0.74*humidity + 4.11*rain + 77.77*hr_sin - 87.33*hr_cos + 
                               50.11*mnth_sin + 63.83*mnth_cos,2)
            
            irradiance_list.append(irradiance) # appending predictions to list to take hourly average
            irradiance_cm_avg = round(average(irradiance_list),2) # taking hourly average
            if irradiance < 0:
                irradiance = 0 # when is night time irradiance is none
            else: 
                pass 
            
            if irradiance_cm_avg < 0:
                irradiance_cm_avg = 0 # when is night time irradiance is none 
            else:
                pass

            # if i % 6 == 0: # Divisible by 6 to get average at every hour (XX:00)
            print("Date: {}".format(time) + "\n")
            print("Temperature: {}".format(temp) + " degrees Celsius")
            print("Wind Speed: {}".format(ws) + " m/s")
            print("Pressure: {}".format(pressure) + " Pa")
            print("Humidity: {}".format(humidity) + " %")
            print("Irradiance: {}".format(irradiance) + " Watt/m2")
            print ("------------------------------------------")
            if i % 6 == 0:
                print("Average Hourly Irradiance: {}".format(irradiance_cm_avg) + " Watt/m2")
                print ("------------------------------------------")
                irradiance_list = [] # reset list as we move into new hour
            else:
                pass
            i += 1

            try:
                with open('sdq_weather_forecasts.csv') as spark_obj:
                    a = 0
                    for line in spark_obj:
                        if a == i:
                            response = line.encode('utf-8')
                            conn.send(response)
                        a += 1
                        pass
                    
            except:
                e = sys.exc_info()[0]
                print("Error: %s" % e)

            sleep(3) # delay by 3 seconds to simulate stream
            
conn.close()

# get_weather()
