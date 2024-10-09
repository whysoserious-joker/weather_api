from flask import Flask
from flask import render_template
from flask import request,session, redirect, url_for, send_from_directory,make_response 
from flask_session import Session
import pandas as pd
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

@app.route('/getstarted')
def getstarted():
    return render_template('input.html')

# endpoint route for static files
@app.route('/static/<path:path>')
def send_static(path):
    return send_from_directory('static', path)



##################################### get_nearest_stations ##########################################
#### /get_nearest_stations?key=123&lat=44.6592&lon=-74.9681&n=15

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
        print(row)
        item={}
        item['station_id']=str(row['station_id'])
        item['station']=str(row['station'])
        item['name']=str(row['name'])
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


###################################### getTableData_nearest_stations ################################
# /getTableData_nearest_stations?key=123&lat=44.6592&lon=-74.9681&n=2

@app.route("/getTableData_nearest_stations",methods=['GET','POST'])
def getTableData_nearest_stations():
    res={}
    
    key=request.args.get("key")
    if key!='123':
        res['code']=0
        res['msg']='Invalid key'
        return json.dumps(res,indent=4)
    
    lat=request.args.get("lat")
    lon=request.args.get("lon")
    n=request.args.get("n")
    # print(given_date,given_zip)
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

    row_df=pd.DataFrame(cur.fetchall())
    res['sql_time']=round(end_time-start_time,2)
    res['req']='/getTableData_nearest_stations'

    df=row_df.reset_index().drop('index',axis=1)
    # table_html = df.to_html(classes='table table-bordered table-striped', index=False)

    return render_template('table3.html', df=df,n = df.shape[0],file_name='nearest_stations')


################################## getData_by_zip_by_date_by_hour #####################################################################
### /getData_by_zip_by_date_by_hour?key=123&zipcode=13676&date=2022-01-01&hour=13

@app.route("/getData_by_zip_by_date_by_hour",methods=['GET','POST'])
def getData_by_zip_by_date_by_hour():
    res={}
    
    key=request.args.get("key")
    if key!='123':
        res['code']=0
        res['msg']='Invalid key'
        return json.dumps(res,indent=4)
    
    given_date=request.args.get("date")
    given_zip=request.args.get("zipcode")
    given_hour=int(request.args.get('hour'))
    date_format="%Y-%m-%d"
    # print(given_date,given_zip)
    try:
        user_date=datetime.datetime.strptime(given_date,date_format)
        user_year=user_date.year
    except:
        res['code']=0
        res['msg']='Invalid date'
        return json.dumps(res,indent=4)
    
    if not ((given_hour >=0 and given_hour <24)):
        res['code']=0
        res['msg']='Invalid hour'
        return json.dumps(res,indent=4)

    get_zip_coords_sql=open('sql_scripts/get_zip_coords.sql').read()
    cur.execute(get_zip_coords_sql,(given_zip))
    for row in cur:
        # print(row['latitude'])
        if (row['latitude'] == None) or (row['longitude'] == None):
            res['code']=0
            res['msg']='Invalid Zip Code'
            return json.dumps(res,indent=4)
        else:
            zip_latitude=float(row['latitude'])
            zip_longitude=float(row['longitude'])

    get_place_names_sql=open('sql_scripts/get_place_names.sql').read()
    cur.execute(get_place_names_sql,(given_zip))
    place_names=[]
    for row in cur:
        place_names.append(row['place_name'])

    get_weather_sql=open('sql_scripts/get_data_by_zip_by_date_by_hour.sql').read()
    start_time=time.time()
    cur.execute(get_weather_sql,(zip_latitude,zip_latitude,
                                 zip_longitude,zip_longitude,
                                 zip_latitude,zip_latitude,
                                 zip_longitude,zip_longitude,user_year,
                                 user_date,given_hour))
    end_time=time.time()
    

    res['code']=1
    res['msg']='ok'

    output=[]
    item={}
    wd=[]
    ws=[]
    wv=[]
    wat=[]
    wap=[]
    for row in cur:
        
        item['station_name']=str(row['station_name'])
        item['station_distance']=str(round(row['distance'],2))+" m"
        item['year']=str(row['given_year'])
        item['month']=str(row['month_of_year'])
        item['week']=str(row['week_of_month'])
        item['hour']=str(row['hour_of_day'])
        item['weather']={}


        item['weather']['wind']={}
        wd=list(set(wd + [str(row['wind_direction_angle'])]))
        item['weather']['wind']['direction']=wd
        item['weather']['wind']['direction_quality']=str(row['wind_direction_quality_code'])
        item['weather']['wind']['type_code']=str(row['wind_type_code'])
        item['weather']['wind']['speed_rate']=str(row['wind_speed_rate'])
        item['weather']['wind']['speed_quality']=str(row['wind_speed_quality_code'])
        item['weather']['wind']['units']={}
        item['weather']['wind']['units']['direction']='Angular Degrees'
        item['weather']['wind']['units']['speed_rate']='meters per second'


        item['weather']['sky']={}
        ws=list(set(ws + [str(row['sky_ceiling_height'])]))
        item['weather']['sky']['ceiling_height']=ws
        item['weather']['sky']['ceiling_quality']=str(row['sky_ceiling_quality'])
        item['weather']['sky']['ceiling_determination']=str(row['sky_ceiling_determination'])
        item['weather']['sky']['ceiling_cavok_code']=str(row['sky_ceiling_cavok_code'])
        item['weather']['sky']['units']={}
        item['weather']['sky']['units']['ceiling height']='meters'


        item['weather']['visibility']={}
        wv=list(set(wv + [str(row['vis_distance_dim'])]))
        item['weather']['visibility']['distance_dimension']=wv
        item['weather']['visibility']['distance_quality']=str(row['vis_distance_quality'])
        item['weather']['visibility']['variability']=str(row['viz_variability'])
        item['weather']['visibility']['quality_variability']=str(row['viz_quality_variability'])
        item['weather']['visibility']['units']={}
        item['weather']['visibility']['units']['distance_dimension']='meters'

        item['weather']['air_temperature']={}
        wat=list(set(wat + [str(row['air_temp'])]))
        item['weather']['air_temperature']['temperature']=wat
        item['weather']['air_temperature']['temp_quality']=str(row['air_temp_quality'])
        item['weather']['air_temperature']['dew_point_temp']=str(row['air_dew'])
        item['weather']['air_temperature']['dew_qulity']=str(row['air_dew_quality'])
        item['weather']['air_temperature']['units']={}
        item['weather']['air_temperature']['units']['temperature']='Degrees Celsius'
        item['weather']['air_temperature']['units']['dew_point_temp']='Degrees Celsius'

        item['weather']['atmospheric_presure']={}
        wap=list(set(wap + [str(row['atm_pressure'])]))
        item['weather']['atmospheric_presure']['sea_level_pressure']=wap
        item['weather']['atmospheric_presure']['quality']=str(row['atm_pressure_quality'])
        item['weather']['atmospheric_presure']['units']={}
        item['weather']['atmospheric_presure']['units']['sea_level_pressure']='Hectopascals'

    output.append(item)

    res['places_under_zipcode']=place_names
    res['results']=output
    res['num_results']=len(output)
    res['sql_time']=round(end_time-start_time,2)
    
    res['req']='/getData_by_zip_by_date_by_hour'
    return json.dumps(res,indent=4)



######################################## getData_by_zip_by_date_all_hours ###################################################################
# /getData_by_zip_by_date_all_hours?key=123&zipcode=13676&date=2022-01-01

@app.route("/getData_by_zip_by_date_all_hours",methods=['GET','POST'])
def getData_by_zip_by_date_all_hours():
    res={}
    
    key=request.args.get("key")
    if key!='123':
        res['code']=0
        res['msg']='Invalid key'
        return json.dumps(res,indent=4)
    
    given_date=request.args.get("date")
    given_zip=request.args.get("zipcode")
    date_format="%Y-%m-%d"
    # print(given_date,given_zip)
    try:
        user_date=datetime.datetime.strptime(given_date,date_format)
        user_year=user_date.year
    except:
        res['code']=0
        res['msg']='Invalid date'
        return json.dumps(res,indent=4)
    


    get_zip_coords_sql=open('sql_scripts/get_zip_coords.sql').read()
    cur.execute(get_zip_coords_sql,(given_zip))
    for row in cur:
        # print(row['latitude'])
        if (row['latitude'] == None) or (row['longitude'] == None):
            res['code']=0
            res['msg']='Invalid Zip Code'
            return json.dumps(res,indent=4)
        else:
            zip_latitude=float(row['latitude'])
            zip_longitude=float(row['longitude'])

    get_place_names_sql=open('sql_scripts/get_place_names.sql').read()
    cur.execute(get_place_names_sql,(given_zip))
    place_names=[]
    for row in cur:
        place_names.append(row['place_name'])

    get_weather_sql=open('sql_scripts/get_data_by_zip_by_date_all_hours.sql').read()
    start_time=time.time()
    cur.execute(get_weather_sql,(zip_latitude,zip_latitude,
                                 zip_longitude,zip_longitude,
                                 zip_latitude,zip_latitude,
                                 zip_longitude,zip_longitude,user_year,user_date))
    end_time=time.time()
    

    res['code']=1
    res['msg']='ok'

    output=[]
    item={}
    n=0
    for row in cur:
        if n==0:
            item['station_name']=str(row['station_name'])
            item['station_distance']=str(round(row['distance'],2))+" m"
            item['year']=str(row['given_year'])
            item['hour']=[]
            d={}
            item['hour'].append(d)
        
        hr=str(row['hour_of_day'])
        item['hour'][0][hr]={}
        item['hour'][0][hr]['weather']={}
        item['hour'][0][hr]['weather']['wind']={}
        item['hour'][0][hr]['weather']['wind']['direction']=str(row['avg_wind_direction_angle'])
        item['hour'][0][hr]['weather']['wind']['speed_rate']=str(row['avg_wind_speed_rate'])
        item['hour'][0][hr]['weather']['wind']['units']={}
        item['hour'][0][hr]['weather']['wind']['units']['direction']='Angular Degrees'
        item['hour'][0][hr]['weather']['wind']['units']['speed_rate']='meters per second'


        item['hour'][0][hr]['weather']['sky']={}
        item['hour'][0][hr]['weather']['sky']['ceiling_height']=str(row['avg_sky_ceiling_height'])
        item['hour'][0][hr]['weather']['sky']['units']={}
        item['hour'][0][hr]['weather']['sky']['units']['ceiling height']='meters'


        item['hour'][0][hr]['weather']['visibility']={}
        item['hour'][0][hr]['weather']['visibility']['distance_dimension']=str(row['avg_vis_distance'])
        item['hour'][0][hr]['weather']['visibility']['units']={}
        item['hour'][0][hr]['weather']['visibility']['units']['distance_dimension']='meters'

        item['hour'][0][hr]['weather']['air_temperature']={}
        item['hour'][0][hr]['weather']['air_temperature']['temperature']=str(row['avg_air_temp'])
        item['hour'][0][hr]['weather']['air_temperature']['dew_point_temp']=str(row['avg_air_dew'])
        item['hour'][0][hr]['weather']['air_temperature']['units']={}
        item['hour'][0][hr]['weather']['air_temperature']['units']['temperature']='Degrees Celsius'
        item['hour'][0][hr]['weather']['air_temperature']['units']['dew_point_temp']='Degrees Celsius'

        item['hour'][0][hr]['weather']['atmospheric_presure']={}
        item['hour'][0][hr]['weather']['atmospheric_presure']['sea_level_pressure']=str(row['avg_atm_pressure'])
        item['hour'][0][hr]['weather']['atmospheric_presure']['units']={}
        item['hour'][0][hr]['weather']['atmospheric_presure']['units']['sea_level_pressure']='Hectopascals'
        n+=1

    output.append(item)

    res['places_under_zipcode']=place_names
    res['results']=output
    res['num_results']=len(output)
    res['sql_time']=round(end_time-start_time,2)
    
    res['req']='/getData_by_zip_by_date_all_hours'
    return json.dumps(res,indent=4)


############################################## getTableData_by_zip_by_date_all_hours #############################################################
# /getTableData_by_zip_by_date_all_hours?key=123&zipcode=13676&date=2022-01-01

@app.route("/getTableData_by_zip_by_date_all_hours",methods=['GET','POST'])
def getTableData_by_zip_by_date_all_hours():
    res={}
    
    key=request.args.get("key")
    if key!='123':
        res['code']=0
        res['msg']='Invalid key'
        return json.dumps(res,indent=4)
    
    given_date=request.args.get("date")
    given_zip=request.args.get("zipcode")
    date_format="%Y-%m-%d"
    # print(given_date,given_zip)
    try:
        user_date=datetime.datetime.strptime(given_date,date_format)
        user_year=user_date.year
    except:
        res['code']=0
        res['msg']='Invalid date'
        return json.dumps(res,indent=4)
    


    get_zip_coords_sql=open('sql_scripts/get_zip_coords.sql').read()
    cur.execute(get_zip_coords_sql,(given_zip))
    for row in cur:
        # print(row['latitude'])
        if (row['latitude'] == None) or (row['longitude'] == None):
            res['code']=0
            res['msg']='Invalid Zip Code'
            return json.dumps(res,indent=4)
        else:
            zip_latitude=float(row['latitude'])
            zip_longitude=float(row['longitude'])

    get_place_names_sql=open('sql_scripts/get_place_names.sql').read()
    cur.execute(get_place_names_sql,(given_zip))
    place_names=[]
    for row in cur:
        place_names.append(row['place_name'])

    get_weather_sql=open('sql_scripts/get_data_by_zip_by_date_all_hours.sql').read()
    start_time=time.time()
    
    
    cur.execute(get_weather_sql,(zip_latitude,zip_latitude,
                                 zip_longitude,zip_longitude,
                                 zip_latitude,zip_latitude,
                                 zip_longitude,zip_longitude,user_year,user_date))
    end_time=time.time()

    df=pd.DataFrame()

    row_df=pd.DataFrame(cur.fetchall())
    if not row_df.empty:
        row_df['zipcode']=place_names[0]
        df=pd.concat([df,row_df],ignore_index=True)
        
    res['num_results']=df.shape[0]
    res['sql_time']=round(end_time-start_time,2)
    
    res['req']='/getTableData_by_zip_by_date_all_hours'
    df=df.reset_index().drop('index',axis=1)
    
    table_html = df.to_html(classes='table table-bordered table-striped', index=False)
    return render_template('table3.html', df=df, n =df.shape[0],file_name='weatherdata_by_zip_by_date_all_hours')





################################## getData_by_station_id_by_date ####################################
# /getData_by_station_id_by_date?key=123&station_id=2473&date=2022-01-01

@app.route("/getData_by_station_id_by_date",methods=['GET','POST'])
def getData_by_station_id_by_date():
    res={}
    
    key=request.args.get("key")
    if key!='123':
        res['code']=0
        res['msg']='Invalid key'
        return json.dumps(res,indent=4)
    
    station_id=request.args.get("station_id")
    given_date=request.args.get("date")
    date_format="%Y-%m-%d"
    # print(given_date,given_zip)
    try:
        user_date=datetime.datetime.strptime(given_date,date_format)
        station_id=int(station_id)

    except:
        res['code']=0
        res['msg']='Invalid entry'
        return json.dumps(res,indent=4)
    

    get_zip_coords_sql=open('sql_scripts/getData_by_station_id_by_date.sql').read()
    start_time=time.time()
    cur.execute(get_zip_coords_sql,(station_id,user_date))
    end_time=time.time()
    

    res['code']=1
    res['msg']='ok'

    output=[]
    for row in cur:
        item={}
        item['station_name']=str(row['name'])
        item['weather']={}
        item['weather']['temp']={}
        item['weather']['temp']['mean_temp']=str(row['air_temp'])
        item['weather']['temp']['max_temp']=str(row['air_temp_max'])
        item['weather']['temp']['min_temp']=str(row['air_temp_min'])
        item['weather']['wind_speed']=str(row['wind_speed_rate'])
        item['weather']['wind_direction_angle']=str(row['wind_direction_angle'])
        item['weather']['max_wind']=str(row['wind_speed_rate_max'])
        item['weather']['sky_ceiling_height']=str(row['sky_ceiling_height'])
        item['weather']['visibility']=str(row['vis_distance_dim'])
        item['weather']['air_dew']=str(row['air_dew'])
        item['weather']['atm_pressure']=str(row['atm_pressure'])
        # item['weather']['precipitation']=str(row['prcp'])
        # item['weather']['frshtt']=get_frshtt(row)
        output.append(item)

    res['results']=output
    res['num_results']=len(output)
    res['sql_time']=round(end_time-start_time,2)
    
    res['req']='/getData_by_station_id_by_date'
    return json.dumps(res,indent=4)



####################################### getData_by_station_id_by_daterange ###############################
# /getData_by_station_id_by_daterange?key=123&station_id=2473&start_date=2022-01-01&end_date=2022-01-02

@app.route("/getData_by_station_id_by_daterange",methods=['GET','POST'])
def getData_by_station_id_by_daterange():
    res={}
    
    key=request.args.get("key")
    if key!='123':
        res['code']=0
        res['msg']='Invalid key'
        return json.dumps(res,indent=4)
    
    station_id=request.args.get("station_id")
    start_date=request.args.get("start_date")
    end_date=request.args.get("end_date")
    date_format="%Y-%m-%d"
    # print(given_date,given_zip)
    try:
        start_date=datetime.datetime.strptime(start_date,date_format)
        end_date=datetime.datetime.strptime(end_date,date_format)
        station_id=int(station_id)

    except:
        res['code']=0
        res['msg']='Invalid entry'
        return json.dumps(res,indent=4)
    

    get_zip_coords_sql=open('sql_scripts/get_data_by_station_id_by_daterange.sql').read()
    start_time=time.time()
    cur.execute(get_zip_coords_sql,(station_id,start_date,end_date))
    end_time=time.time()
    

    res['code']=1
    res['msg']='ok'

    output=[]
    n=0
    item={}
    item['weather']={}
    item['weather']['dates']={}
    for row in cur:
        # print(row)
        if n==0:
            item['station_name']=str(row['name'])
        date=str(row['date'])
        item['weather']['dates'][date]={}
        item['weather']['dates'][date]['temp']={}
        item['weather']['dates'][date]['temp']['mean_temp']=str(row['air_temp'])
        item['weather']['dates'][date]['temp']['max_temp']=str(row['air_temp_max'])
        item['weather']['dates'][date]['temp']['min_temp']=str(row['air_temp_min'])
        item['weather']['dates'][date]['wind']={}
        item['weather']['dates'][date]['wind']['wind_speed']=str(row['wind_speed_rate'])
        item['weather']['dates'][date]['wind']['wind_direction_angle']=str(row['wind_direction_angle'])
        item['weather']['dates'][date]['wind']['max_wind']=str(row['wind_speed_rate_max'])
        item['weather']['dates'][date]['sky_ceiling_height']=str(row['sky_ceiling_height'])
        item['weather']['dates'][date]['visibility']=str(row['vis_distance_dim'])
        item['weather']['dates'][date]['air_dew']=str(row['air_dew'])
        item['weather']['dates'][date]['atm_pressure']=str(row['atm_pressure'])
        # item['weather']['dates'][date]['precipitation']=str(row['prcp'])
        # item['weather']['dates'][date]['frshtt']=get_frshtt(row)
        n+=1

    output.append(item)

    res['results']=output
    res['num_results']=len(output)
    res['sql_time']=round(end_time-start_time,2)
    
    res['req']='/getData_by_station_id_by_daterange'
    return json.dumps(res,indent=4)


###################################### getTableData_by_station_id_by_daterange ################################
# /getTableData_by_station_id_by_daterange?key=123&station_id=7&start_date=2010-01-01&end_date=2010-01-02
@app.route("/getTableData_by_station_id_by_daterange",methods=['GET','POST'])
def getTableData_by_station_id_by_daterange():
    res={}
    
    key=request.args.get("key")
    if key!='123':
        res['code']=0
        res['msg']='Invalid key'
        return json.dumps(res,indent=4)
    
    station_id=request.args.get("station_id")
    start_date=request.args.get("start_date")
    end_date=request.args.get("end_date")
    date_format="%Y-%m-%d"
    # print(given_date,given_zip)
    try:
        start_date=datetime.datetime.strptime(start_date,date_format)
        end_date=datetime.datetime.strptime(end_date,date_format)
        station_id=int(station_id)

    except:
        res['code']=0
        res['msg']='Invalid entry'
        return json.dumps(res,indent=4)
    

    get_zip_coords_sql=open('sql_scripts/get_data_by_station_id_by_daterange.sql').read()
    start_time=time.time()
    cur.execute(get_zip_coords_sql,(station_id,start_date,end_date))
    end_time=time.time()
    

    res['code']=1
    res['msg']='ok'

    
    row_df=pd.DataFrame(cur.fetchall())
    res['sql_time']=round(end_time-start_time,2)
    res['req']='/getTableData_by_station_id_by_daterange'
    df=row_df.reset_index().drop('index',axis=1)
    
    # table_html = df.to_html(classes='table table-bordered table-striped', index=False)

    return render_template('table3.html', df=df,n = df.shape[0],file_name='weatherdata_by_station_by_daterange')



######################################## getData_by_zip_by_date ##############################
# /getData_by_zip_by_date?key=123&zipcode=13676&date=2022-01-01

@app.route("/getData_by_zip_by_date",methods=['GET','POST'])
def getData_by_zip_by_date():
    res={}
    key=request.args.get("key")
    if key!='123':
        res['code']=0
        res['msg']='Invalid key'
        return json.dumps(res,indent=4)
    
    given_date=request.args.get("date")
    given_zip=request.args.get("zipcode")
    date_format="%Y-%m-%d"
    # print(given_date,given_zip)
    try:
        user_date=datetime.datetime.strptime(given_date,date_format)
        user_year=user_date.year
    except:
        res['code']=0
        res['msg']='Invalid date'
        return json.dumps(res,indent=4)
    

    get_zip_coords_sql=open('sql_scripts/get_zip_coords.sql').read()
    cur.execute(get_zip_coords_sql,(given_zip))
    for row in cur:
        # print(row['latitude'])
        if (row['latitude'] == None) or (row['longitude'] == None):
            res['code']=0
            res['msg']='Invalid Zip Code'
            return json.dumps(res,indent=4)
        else:
            zip_latitude=float(row['latitude'])
            zip_longitude=float(row['longitude'])

    get_place_names_sql=open('sql_scripts/get_place_names.sql').read()
    cur.execute(get_place_names_sql,(given_zip))
    place_names=[]
    for row in cur:
        place_names.append(row['place_name'])

    get_weather_sql=open('sql_scripts/get_data_by_zip_by_date.sql').read()
    start_time=time.time()
    cur.execute(get_weather_sql,(user_date,zip_latitude,zip_latitude,
                                 zip_longitude,zip_longitude,
                                 zip_latitude,zip_latitude,
                                 zip_longitude,zip_longitude,
                                 user_year))
    end_time=time.time()
    

    res['code']=1
    res['msg']='ok'

    output=[]
    for row in cur:
        item={}
        item['station_name']=str(row['name'])
        # item['station_distance']=str(round(row['distance'],2))+" m"
        # item['year']=str(row['year'])
        # item['month']=str(row['month'])
        item['weather']={}
        item['weather']['temp']={}
        item['weather']['temp']['mean_temp']=str(row['air_temp'])
        item['weather']['temp']['max_temp']=str(row['air_temp_max'])
        item['weather']['temp']['min_temp']=str(row['air_temp_min'])
        item['weather']['wind_speed']=str(row['wind_speed_rate'])
        item['weather']['wind_direction_angle']=str(row['wind_direction_angle'])
        item['weather']['max_wind']=str(row['wind_speed_rate_max'])
        item['weather']['sky_ceiling_height']=str(row['sky_ceiling_height'])
        item['weather']['visibility']=str(row['vis_distance_dim'])
        item['weather']['air_dew']=str(row['air_dew'])
        item['weather']['atm_pressure']=str(row['atm_pressure'])
        output.append(item)

    res['places_under_zipcode']=place_names
    res['results']=output
    res['num_results']=len(output)
    res['sql_time']=round(end_time-start_time,2)
    
    res['req']='/getData_by_zip_by_date'
    return json.dumps(res,indent=4)

###################################### getData_by_placename_by_date #############################################
# /getData_by_placename_by_date?key=123&place_name=potts&country_code=us&date=2022-01-01

@app.route("/getData_by_placename_by_date",methods=['GET','POST'])
def getData_by_placename_by_date():
    res={}
    
    key=request.args.get("key")
    if key!='123':
        res['code']=0
        res['msg']='Invalid key'
        return json.dumps(res,indent=4)
    
    given_date=request.args.get("date")

    given_place=request.args.get("place_name")
    given_place=f'{given_place.lower()}%'
    # print(given_place)

    given_country_code=request.args.get("country_code")
    given_country_code=given_country_code.lower()

    date_format="%Y-%m-%d"
    # print(given_date,given_zip)
    try:
        user_date=datetime.datetime.strptime(given_date,date_format)
        user_year=user_date.year
    except:
        res['code']=0
        res['msg']='Invalid date'
        return json.dumps(res,indent=4)
    

    get_zip_coords_sql=open('sql_scripts/get_place_coords.sql').read()
    cur.execute(get_zip_coords_sql,(given_place,given_country_code))

    coordinates={}
    for row in cur:
        if (row['latitude'] == None) or (row['longitude'] == None):
            res['code']=0
            res['msg']='Place name doesn\'t match with any'
            return json.dumps(res,indent=4)
        else:
            p=row['place_name']
            coordinates[p]={}
            coordinates[p]['latitude']=float(row['latitude'])
            coordinates[p]['longitude']=float(row['longitude'])

    records=[]
    for place,coords in coordinates.items():
        latitude=coords['latitude']
        longitude=coords['longitude']

        get_weather_sql=open('sql_scripts/get_data_by_zip_by_date.sql').read()
        start_time=time.time()
        sql_to_execute = cur.mogrify(get_weather_sql,(user_date,latitude,latitude,
                                 longitude,longitude,
                                 latitude,latitude,
                                 longitude,longitude,
                                 user_year))
        cur.execute(sql_to_execute)
        end_time=time.time()
        

        res['code']=1
        res['msg']='ok'

        output=[]
        for row in cur:
            item={}
            item['place_name']=place
            item['station_name']=str(row['name'])
            item['weather']={}
            item['weather']['temp']={}
            item['weather']['temp']['mean_temp']=str(row['air_temp'])
            item['weather']['temp']['max_temp']=str(row['air_temp_max'])
            item['weather']['temp']['min_temp']=str(row['air_temp_min'])
            item['weather']['wind_speed']=str(row['wind_speed_rate'])
            item['weather']['wind_direction_angle']=str(row['wind_direction_angle'])
            item['weather']['max_wind']=str(row['wind_speed_rate_max'])
            item['weather']['sky_ceiling_height']=str(row['sky_ceiling_height'])
            item['weather']['visibility']=str(row['vis_distance_dim'])
            item['weather']['air_dew']=str(row['air_dew'])
            item['weather']['atm_pressure']=str(row['atm_pressure'])
            output.append(item)

        records.append(output)
        
    res['results']=records
    res['num_results']=len(records)
    res['sql_time']=round(end_time-start_time,2)
    
    res['req']='/getData_by_placename_by_date'
    return json.dumps(res,indent=4)


################################ getTableData_by_placename_by_date ###############################################
# /getTableData_by_placename_by_date?key=123&place_name=potts&country_code=us&date=2022-01-01
@app.route("/getTableData_by_placename_by_date",methods=['GET','POST'])
def getTableData_by_placename_by_date():
    res={}
    
    key=request.args.get("key")
    if key!='123':
        res['code']=0
        res['msg']='Invalid key'
        return json.dumps(res,indent=4)
    
    given_date=request.args.get("date")

    given_place=request.args.get("place_name")
    given_place=f'{given_place.lower()}%'
    # print(given_place)

    given_country_code=request.args.get("country_code")
    given_country_code=given_country_code.lower()

    date_format="%Y-%m-%d"

    try:
        user_date=datetime.datetime.strptime(given_date,date_format)
        user_year=user_date.year
    except:
        res['code']=0
        res['msg']='Invalid date'
        return json.dumps(res,indent=4)

    get_zip_coords_sql=open('sql_scripts/get_place_coords.sql').read()
    cur.execute(get_zip_coords_sql,(given_place,given_country_code))

    coordinates={}

    for row in cur:
        if (row['latitude'] == None) or (row['longitude'] == None):
            res['code']=0
            res['msg']='Place name doesn\'t match with any'
            return json.dumps(res,indent=4)
        else:
            p=row['place_name']
            coordinates[p]={}
            coordinates[p]['latitude']=float(row['latitude'])
            coordinates[p]['longitude']=float(row['longitude'])
    # print(coordinates)
    records=[]
    cols=['station_id','date',
                                  'year',
                                  'month',
                                  'day',
                                  'mean_temp',
                                  'wind_speed',
                                  'max_wind_speed',
                                  'max_temp',
                                  'min_temp',
                                  'precipitation',
                                  'fog',
                                  'rain',
                                  'snow',
                                  'hail',
                                  'thunder',
                                  'tornado',
                                  'station_name',
                                  'station-distance',
                                  'country_name',
                                  'latitude',
                                  'longitude',
                                  'place_name'
                                  ]
    df=pd.DataFrame()
    for place,coords in coordinates.items():
        latitude=coords['latitude']
        longitude=coords['longitude']

        get_weather_sql=open('sql_scripts/get_data_by_zip_by_date.sql').read()
        start_time=time.time()
        sql_to_execute = cur.mogrify(get_weather_sql,(user_date,latitude,latitude,
                                 longitude,longitude,
                                 latitude,latitude,
                                 longitude,longitude,
                                 user_year))
        cur.execute(sql_to_execute)
        end_time=time.time()
        row_df=pd.DataFrame(cur.fetchall())

        if not row_df.empty:
            row_df['place_name']=place
            # row_df.columns=cols
            df=pd.concat([df,row_df],ignore_index=True)
            # a=cols[:-1]
            # a.insert(0,cols[-1])
            # df=df[a]
        
    res['num_results']=df.shape[0]
    res['sql_time']=round(end_time-start_time,2)
    
    res['req']='/getTableData_by_placename_by_date'
    df=df.reset_index().drop('index',axis=1)
    
    # table_html = df.to_html(classes='table table-bordered table-striped', index=False)
    return render_template('table3.html', df=df, n =df.shape[0],file_name='weatherdata_by_place_by_date')


if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True)
