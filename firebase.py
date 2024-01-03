import pandas as pd
import numpy as np
import pyrebase         # Connect ke firebase realtime database
import psycopg2         # Connect ke postgreSQL
from db import *
from datetime import datetime
from reqs import config,db_pos

try:
    # Firebase
    firebase = pyrebase.initialize_app(config)
    db =firebase.database()
    # Postgres
    conn = psycopg2.connect(f"dbname={db_pos['DB_NAME']} user={db_pos['user']} password={db_pos['password']}")
    cur = conn.cursor()
    query = """INSERT INTO node1 (tanggal,arah_angin,cuaca,hujan,humidity,kecepatan_angin,tekanan_udara,temperature,tingkat_cahaya,tingkat_gerimis) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
    # Pengambilan Data
    firebase_data = list(db.child("Node1").get().val().values())
    for i in firebase_data:
        TIMESTAMP_TEMP = i['Tanggal'] +" "+i['Waktu']
        datetime_convert = datetime.strptime(TIMESTAMP_TEMP,'%A,%d-%B-%Y %H:%M:%S')
        data= (datetime_convert,(i['Arah Angin']),(i['Cuaca']),float(i['Hujan']),int(i['Humadity']),float(i['Kecepatan Angin ms']), float(i['Tekanan Udara']), int(i['Temperature']), int(i['Tingkat Cahaya']), int(i['Tingkat Gerimis']))
        cur.execute(query,data)
        conn.commit()
        print("Record inserted successfully into mobile table")
        
except Exception as error:
    print(error)
        

# Hujan,Humadity,Tanggal,Tekanan Udara,Temperature,Tingkat Cahaya, Waktu
# firebase_data = list(db.child("Node1").child('test').get().val().values())