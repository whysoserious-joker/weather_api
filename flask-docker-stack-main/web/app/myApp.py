from flask import Flask
from flask import render_template
from flask import request,session, redirect, url_for, send_from_directory,make_response 
from flask_session import Session
from datetime import timedelta,datetime, date
import datetime
import time
import json,os
import pymysql
import prebuilt_loggers



app_log = prebuilt_loggers.filesize_logger('logs/app.log')
#create Flask app instance
app = Flask(__name__,static_url_path='')
application = app

app.secret_key = 'xfdgbsbW$^%W%Hwe57hE56yw56h'
app.config['SESSION_TYPE'] = 'filesystem'

sess = Session()
sess.init_app(app)

def get_frshtt(row):
    frshtt={'fog':0,
            'rain':0,
            'snow':0,
            'hail':0,
            'thunder':0,
            'tornado':0
            }
    if row['fog']=='1':
        frshtt['fog']=1
    if row['rain']=='1':
        frshtt['rain']=1
    if row['snow']=='1':
        frshtt['snow']=1
    if row['hail']=='1':
        frshtt['hail']=1
    if row['thunder']=='1':
        frshtt['thunder']=1
    if row['tornado']=='1':
        frshtt['tornado']=1
    return frshtt

db_config = {
    'host': 'apps.clarksonmsda.org',
    'user': 'clarkson24',
    'password': 'clark@2024',
    'database': 'weather_info',
    'autocommit':True
}

conn=pymysql.connect(**db_config)
cur = conn.cursor(pymysql.cursors.DictCursor)

@app.route('/')
def index():
    return render_template('index.html')


# endpoint route for static files
@app.route('/static/<path:path>')
def send_static(path):
    return send_from_directory('static', path)



##################################### get_nearest_stations ##########################################
@app.route("/get_nearest_stations",methods=['GET','POST'])
def get_nearest_stations():
    res={}
    key=request.args.get("key")
    if key!='123':
        res['code']=0
        res['msg']='Invalid key'
        return json.dumps(res,indent=4)
    
    lat=request.args.get("lat")
    lon=request.args.get("lon")
    n=request.args.get("n")
    try:
        latitude=float(lat)
        longitude=float(lon)
        n=int(n)
    except:
        res['code']=0
        res['msg']='Invalid entry'
        return json.dumps(res,indent=4)

    get_zip_coords_sql=open('sql_scripts/get_nearest_stations.sql').read()
    start_time=time.time()
    cur.execute(get_zip_coords_sql,(latitude,latitude,
                                 longitude,longitude,
                                 latitude,latitude,
                                 longitude,longitude,
                                 n))
    end_time=time.time()
    

    res['code']=1
    res['msg']='ok'

    output=[]
    for row in cur:
        item={}
        item['station_id']=str(row['station_id'])
        item['station_name']=str(row['station_name'])
        item['country_code']=str(row['country_code'])
        item['latitude']=str(row['latitude'])
        item['longitude']=str(row['longitude'])
        item['station_distance']=str(round(row['distance'],2))+" m"
        output.append(item)

    res['results']=output
    res['num_results']=len(output)
    res['sql_time']=round(end_time-start_time,2)
    
    res['req']='/get_nearest_stations'
    return json.dumps(res,indent=4)

if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True)
