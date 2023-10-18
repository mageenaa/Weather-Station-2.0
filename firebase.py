import pandas as pd
import numpy as np
import pyrebase
import psycopg2
from db import *
from datetime import datetime

# config = {
#     "apiKey" : "AIzaSyC0NT17TukxEKnRse5TwzbZNthH03JR52g",
#     "authDomain" : "iot-colabs.firebaseapp.com",
#     "databaseURL" : "https://iot-colabs-default-rtdb.asia-southeast1.firebasedatabase.app",
#     "projectId" : "iot-colabs",
#     "storageBucket" : "iot-colabs.appspot.com",
#     "messagingSenderId" : "1052600480021",
#     "appId" : "1:1052600480021:web:95dd9ef2191d2de5e6b91c",
#     "measurementId" : "G-XB31HM4TRM"
# }
config = {
    "apiKey": "AIzaSyC0NT17TukxEKnRse5TwzbZNthH03JR52g",
    "authDomain": "iot-colabs.firebaseapp.com",
    "databaseURL": "https://iot-colabs-default-rtdb.asia-southeast1.firebasedatabase.app",
    "storageBucket": "iot-colabs.appspot.com"
}

db_pos = {
    'DB_NAME': 'node1',
    'user': 'postgres',
    'password': '30230745'
}
try:
    # Firebase
    firebase = pyrebase.initialize_app(config)
    db =firebase.database()
    # Postgres
    conn = psycopg2.connect(f"dbname={db_pos['DB_NAME']} user={db_pos['user']} password={db_pos['password']}")
    cur = conn.cursor()
    query = """INSERT INTO prototype (tanggal,arah_angin,hujan,kelembaban,tekanan_udara,temperature,tingkat_cahaya) VALUES (%s,%s,%s,%s,%s,%s,%s)"""
    # Pengambilan Data
    firebase_data = list(db.child("Anjas").child('percobaan').get().val().values())
    # firebase_data = db.child("Anjas").child('percobaan').get().val().values()
    for i in firebase_data:
        TIMESTAMP_TEMP = i['Tanggal'] +" "+i['Waktu']
        datetime_convert = datetime.strptime(TIMESTAMP_TEMP,'%A,%d-%B-%Y %H:%M:%S')
        data= (datetime_convert,(i['Arah Angin']),int(i['Hujan']),int(i['Humadity']),float(i['Tekanan Udara']),int(i['Temperature']),int(i['Tingkat Cahaya']))
        cur.execute(query,data)
        conn.commit()
        print("Record inserted successfully into mobile table")
        
except Exception as error:
    print(error)
        

# Hujan,Humadity,Tanggal,Tekanan Udara,Temperature,Tingkat Cahaya, Waktu