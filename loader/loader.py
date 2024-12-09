import json
import pandas as pd
import os
import glob
from shapely.geometry import Polygon
import yaml
from shapely import Point
import datetime
import paramiko
import time
from sqlalchemy import create_engine,event,text
import urllib.parse
import csv
import time
import logging
from logging.handlers import RotatingFileHandler

logger = logging.getLogger('my_logger')
logger.setLevel(logging.DEBUG)
# Create a file handler which rotates after every 5 MB
handler = RotatingFileHandler('my_log.log', maxBytes=5*1024*1024, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


class loader:
    def __init__(self): 
        self.config=None
        self.path=None
        self.year=None
        self.period=None
        self.coordinates=None
        self.polygon=None
        self.no_of_files=0
        self.st_files=[]
        self.st_files_count=0
        self.ssh_client=None
        self.ftp=None
        self.download_path=None
        self.required_cols=['STATION',
                   'DATE',
                   'LATITUDE',
                   'LONGITUDE',
                   'NAME',
                   'WND',
                   'CIG',
                   'VIS',
                   'TMP',
                   'DEW',
                   'SLP',
                   'KA1',
                   'KA2',
                   'MA1',
                   'MD1',
                   'OC1',
                   'OD1']
        # self.dfs=[]
        # self.frame=None
        self.station_details=[]
        self.engine=None
        self.wdf=None

        with open('config.yml','r') as stream:
            self.config=yaml.safe_load(stream)
        self.create_polygon()

    
    def create_polygon(self): # create a polygon based on the given coordinates in config file
        logger.info("Creating polygon from given coordinates...")
        self.coordinates = self.config['load']['polygon'][0]
        for i in range(len(self.coordinates)):
            self.coordinates[i] = self.coordinates[i][::-1]
        try:
             self.polygon = Polygon(self.coordinates)
        except Exception as e:
            logger.debug(f"Error creating polygon: {e}")
            print("Error creating polygon:",e)


    
    def get_stfiles(self): # get list of files whose stations fall inside the polygon
        logger.info("Get station file names inside polygon from index file")
        file_count=0
        with open('index.txt','r') as f:
            for line in f:
                data=json.loads(line)
                if int(data['year']) in range(self.config['load']['start_year'],self.config['load']['end_year']):
                    # print(data['year'])
                    try:
                        lat=float(data['lat'])
                        lon=float(data['lon'])
                    except Exception as e:
                        print(e)
                    
                    point = Point(lat, lon)
                    if self.polygon.contains(point):
                        self.st_files.append(data['file_name'])
                        file_count+=1
                    self.st_files_count=file_count
        logger.debug(f"{self.st_files_count} files to be downloaded")
    

    
    def connect_filesworkpace(self): # connect to filesworkspace server
        logger.info("Connecting to filesworkspace...")
        self.ssh_client = paramiko.SSHClient()
        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            self.ssh_client.connect(hostname=self.config['files_workspace']['host'],
                            port=self.config['files_workspace']['port'],
                            username=self.config['files_workspace']['username'],
                            password=self.config['files_workspace']['password'])
            
            self.ftp=self.ssh_client.open_sftp()
            return 'Connected'
        except Exception as e:
            return Exception

    
    def create_dir(self): # create directories/folders to store downloaded files
        logger.info("Creating directories to store station files")
        for year in range(self.config['load']['start_year'],self.config['load']['end_year']):
            dir_path=os.path.join('csv',self.config['load']['period'],str(year))
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
            else:
                print("Folder already exists")


    
    def download_files(self): # once connected to filesworkspace server, download the st_files 
        self.get_stfiles()
        conn = self.connect_filesworkpace()
        if conn=="Connected":
            st=time.time()
            self.create_dir()
            n=0
            logger.info("Downloading station files from workspace")
            for year in range(self.config['load']['start_year'],self.config['load']['end_year']):
                x=0
                path=os.path.join(self.config['files_workspace']['path'],str(year))
                logger.debug(f"Downloading to path {path}")
                for file in self.ftp.listdir(path):
                    if file in self.st_files:
                        fp=os.path.join(path,file)
                        self.download_path=os.path.join('csv',self.config['load']['period'],str(year))
                        self.ftp.get(fp,os.path.join(self.download_path,file))
                        n+=1
                        x+=1
                        if n%15==0:
                            logger.debug(f"{n} files downloaded")
            self.ssh_client.close()
            logger.debug(f"Time taken to download csv files from workspace:{time.time()-st}")


    def mysql_connect(self):
        logger.info("Connecting to mysql...")
        username = self.config['db']['username']
        password = self.config['db']['password']
        hostname = self.config['db']['hostname']
        port = self.config['db']['port']
        database_name = self.config['db']['database_name']
        encoded_password = urllib.parse.quote_plus(password)

        connection_string = f'mysql+pymysql://{username}:{encoded_password}@{hostname}:{port}/{database_name}'
        print(connection_string)
        try:
            self.engine = create_engine(connection_string)
            self.engine.connect()
            logger.info("Connected to mysql")
            print("Connected to mysql")
        except Exception as e:
            logger.debug(f"Connection to myqsl Unsuccessful {e}")
            print("Connection to myqsl Unsuccessful", e)

        @event.listens_for(self.engine, "before_cursor_execute")
        def receive_before_cursor_execute(
            conn, cursor, statement, params, context, executemany
                ):
                    if executemany:
                        cursor.fast_executemany = True

        

    def merge_files(self,year):
        logger.info("Starting to merge csv files to a dataframe..")
        st=time.time()
        dfs=[]
        path='csv'
        period=self.config['load']['period']
        pp=os.path.join(path,period)
        n=0
        logger.debug(f"Merging csv files for year {year}")
        yp=os.path.join(pp,year)
        for file in os.listdir(yp):
            n+=1
            fp=os.path.join(yp,file)
            logger.debug(f"reading file {fp}")
            df=pd.read_csv(fp)
            logger.debug(f"File shape {df.shape}")
            for col in self.required_cols:
                if col not in df.columns:
                    df[col]=999999
            dfs.append(df[self.required_cols])
        self.frame = pd.concat(dfs, axis=0, ignore_index=True)
        self.clean_frame()
        logger.info(f"Merging completed in {time.time()-st}")
        

    def clean_frame(self):
        logger.info("Cleaning df")
        self.frame[['WIND_DIRECTION_ANGLE','WIND_DIRECTION_QUALITY_CODE','WIND_TYPE_CODE','WIND_SPEED_RATE','WIND_SPEED_QUALITY_CODE']]=self.frame['WND'].str.split(',',expand=True)
        self.frame[['SKY_CEILING_HEIGHT','SKY_CEILING_QUALITY','SKY_CEILING_DETERMINATION','SKY_CEILING_CAVOK_CODE']]=self.frame['CIG'].str.split(',',expand=True)
        self.frame[['VIS_DISTANCE_DIM','VIS_DISTANCE_QUALITY','VIZ_VARIABILITY','VIZ_QUALITY_VARIABILITY']]=self.frame['VIS'].str.split(',',expand=True)
        self.frame[['AIR_TEMP','AIR_TEMP_QUALITY']]=self.frame['TMP'].str.split(',',expand=True)
        self.frame[['AIR_DEW','AIR_DEW_QUALITY']]=self.frame['DEW'].str.split(',',expand=True)
        self.frame[['ATM_PRESSURE','ATM_PRESSURE_QUALITY']]=self.frame['SLP'].str.split(',',expand=True)
        if self.frame['NAME'].isna().any():
                # If NaN values exist, fill NaN values in the split columns with 'NAN'
                split_cols = pd.DataFrame(index=self.frame.index, columns=[0, 1], data='NAN')
        else:
                # Otherwise, split the 'NAME' column
                split_cols = self.frame['NAME'].str.split(', ', expand=True)
        self.frame[['NAME', 'COUNTRY_CODE']] = split_cols
        self.frame=self.frame.drop(['WND','CIG','VIS','TMP','DEW','SLP'],axis=1)
        
        self.frame['COUNTRY_CODE']=self.frame['COUNTRY_CODE'].apply(lambda x: str(x)[-2:])
        self.frame['DATE']=pd.to_datetime(self.frame['DATE'],format='mixed')
        self.frame['STATION']=self.frame['STATION'].astype(str)

        cols_order=['STATION', 'DATE', 'LATITUDE', 'LONGITUDE', 'NAME','COUNTRY_CODE',  'WIND_DIRECTION_ANGLE',
       'WIND_DIRECTION_QUALITY_CODE', 'WIND_TYPE_CODE', 'WIND_SPEED_RATE',
       'WIND_SPEED_QUALITY_CODE', 'SKY_CEILING_HEIGHT', 'SKY_CEILING_QUALITY',
       'SKY_CEILING_DETERMINATION', 'SKY_CEILING_CAVOK_CODE',
       'VIS_DISTANCE_DIM', 'VIS_DISTANCE_QUALITY', 'VIZ_VARIABILITY',
       'VIZ_QUALITY_VARIABILITY', 'AIR_TEMP', 'AIR_TEMP_QUALITY', 'AIR_DEW',
       'AIR_DEW_QUALITY', 'ATM_PRESSURE', 'ATM_PRESSURE_QUALITY','KA1', 'KA2', 'MA1',
       'MD1', 'OC1', 'OD1']
        self.frame=self.frame[cols_order]
        


    def load_station_details(self):
        logger.info("Starting : Load station details")
        st=time.time()
        ext='csv'
        path=os.path.join("csv",self.config['load']['period'])
        for year in range(self.config['load']['start_year'],self.config['load']['end_year']):
            logger.debug(f"Loading year {year}")
            yp=os.path.join(path,str(year))
            for file in os.listdir(yp):
                csv_fp=os.path.join(yp,file)
                if ext in csv_fp:
                    with open(csv_fp, 'r') as f:
                        rf = csv.reader(f)
                        x = 0
                        for row in rf:
                            if x == 0:
                                header = [s.strip('"') for s in row]
                                dt_idx=header.index('DATE')
                                st_idx = header.index('STATION')
                                name_idx = header.index('NAME')
                                lat_idx = header.index('LATITUDE')
                                lon_idx = header.index('LONGITUDE')
                            if x==1:
                                sd={}
                                date=row[dt_idx].strip('"')
                                dto=datetime.datetime.strptime(date,'%Y-%m-%dT%H:%M:%S')
                                sd['year']=dto.year
                                sd["station"] = row[st_idx].strip('"')
                                sd["name"] = row[name_idx].strip('"')
                                sd["latitude"] = row[lat_idx].strip('"')
                                sd['longitude']=row[lon_idx].strip('"')
                                # print(sd)
                                self.station_details.append(sd)
                            x+=1
                            if x>1:
                                break

        df=pd.DataFrame(self.station_details)
        df=df.drop_duplicates()
        df[['name','country_code']]=df['name'].str.split(', ',expand=True)
        df['country_code']=df['country_code'].apply(lambda x: str(x)[-2:])

        logger.debug(f"Station df size {df.shape} ,adding country names")
        c=open('additional_files/country_list.txt','r')
        s=c.read()
        country_list=s.split("\n")
        country_code=[]
        country_name=[]
        n=0
        for item in country_list:
            if n>1:
                s=item.replace(' ','')
                country_code.append(s[0:2])
                country_name.append(s[2:])
            n+=1
        
        country_df=pd.DataFrame({'country_code':country_code,'country_name':country_name})
        self.station_info=df.merge(country_df,on=['country_code'],how='left')
        self.station_info = self.station_info.rename(columns={col: col.lower() for col in self.station_info.columns})
        col_order=['year','station','name','latitude','longitude','country_code','country_name']
        self.station_info=self.station_info[col_order]

        self.station_info['station_id'],_ = pd.factorize(self.station_info['station'])
        self.mysql_connect()
        self.station_info.to_csv('stations.csv',index=False)
        logger.debug(f"station_df {self.station_info.shape} loading")
        self.station_info.to_sql('stations',con = self.engine,if_exists='replace',index=False)
        logger.debug(f"Successfully loaded, time taken {st-time.time()}")
        #     # CREATE UNIQUE INDEX station_id_location_index
        #     # ON weather_hourly.stations_info (station_id, longitude,latitude);
        #     print('Stations info loaded successfully')

    

    def load_weather_data(self):
        logger.info("Loading weather data")
        logger.debug("Reading station index file")
        station_ids=pd.read_csv('stations.csv',dtype={'station': str})  
        station_ids=station_ids[['station_id','station']].drop_duplicates()  

        for year in range(2014,2021): # change this as per your years you want to load.
            logger.debug(f"Working on year {year}")
            self.merge_files(str(year))
            logger.debug("joining with station index df to get index")
            self.wdf=self.frame.merge(station_ids,right_on=['station'],left_on=['STATION'],how='left')
            self.wdf=self.wdf[['station_id','STATION', 'DATE', 'LATITUDE', 'LONGITUDE', 'NAME', 'COUNTRY_CODE',
                                'WIND_DIRECTION_ANGLE', 'WIND_DIRECTION_QUALITY_CODE', 'WIND_TYPE_CODE',
                                'WIND_SPEED_RATE', 'WIND_SPEED_QUALITY_CODE', 'SKY_CEILING_HEIGHT',
                                'SKY_CEILING_QUALITY', 'SKY_CEILING_DETERMINATION',
                                'SKY_CEILING_CAVOK_CODE', 'VIS_DISTANCE_DIM', 'VIS_DISTANCE_QUALITY',
                                'VIZ_VARIABILITY', 'VIZ_QUALITY_VARIABILITY', 'AIR_TEMP',
                                'AIR_TEMP_QUALITY', 'AIR_DEW', 'AIR_DEW_QUALITY', 'ATM_PRESSURE',
                                'ATM_PRESSURE_QUALITY', 'KA1', 'KA2', 'MA1', 'MD1', 'OC1', 'OD1']]
            
            self.wdf.columns=list(x.lower() for x in self.wdf.columns)
            self.wdf = self.wdf.drop_duplicates(subset=['station_id', 'date'], keep='first')
            logger.debug(f"After removing duplicates df shape {self.wdf.shape}")
            chunksize = 50000
            logger.debug("inserting to table in {chunksize} chunks")
            self.mysql_connect()
            for i in range(0, len(self.wdf), chunksize):
                logger.debug(f"insert {i} rows remaining {self.wdf.shape[0]-i}")
                chunk = self.wdf.iloc[i:i+chunksize]
                chunk.to_sql('weather_hourly', con=self.engine, if_exists='append', index=False)
            logger.info("Successfully inserted weather data for year {year}")

            # CREATE UNIQUE INDEX station_id_date_index ON weather_info.weather_hourly (station_id, date);
            # print(f'Weather houlry data loaded successfullly for year {year} and rows {self.wdf.shape[0]}')
            


    # def insert_station_details(self):
    #     print("loading station details...")
    #     station_details=self.frame[['STATION','NAME','COUNTRY_CODE','LATITUDE','LONGITUDE']].drop_duplicates().reset_index(drop=True)
    #     station_details['STATION_ID']=station_details.index+1
    #     station_details=station_details[['STATION_ID','STATION','NAME','COUNTRY_CODE','LATITUDE','LONGITUDE']]

    #     c=open('additional_files/country_list.txt','r')
    #     s=c.read()
    #     country_list=s.split("\n")
    #     country_code=[]
    #     country_name=[]
    #     n=0
    #     for item in country_list:
    #         if n>1:
    #             s=item.replace(' ','')
    #             country_code.append(s[0:2])
    #             country_name.append(s[2:])
    #         n+=1
        
    #     country_df=pd.DataFrame({'COUNTRY_CODE':country_code,'COUNTRY_NAME':country_name})
    #     self.station_info=station_details.merge(country_df,on=['COUNTRY_CODE'],how='left')
    #     self.station_info = self.station_info.rename(columns={col: col.lower() for col in self.station_info.columns})
        
    #     self.stations=pd.concat([self.station_info,self.stations],ignore_index=True).drop_duplicates()

    #     self.mysql_connect()
    #     self.stations.to_sql('stations',con = self.engine,if_exists='replace',index=False)


    def insert_zipcodes(self):
        print("loading zipcodes")
        zipcodes=pd.read_csv('additional_files/geonames-postal.csv',on_bad_lines='skip',delimiter=';')
        zipcodes=zipcodes.drop(['admin name3','admin code3','accuracy'],axis=1)
        cols=['country_code','postal_code','place_name','state','state_code','county','county_code','latitude','longitude','coordinates']
        zipcodes.columns=cols
        self.mysql_connect()
        zipcodes.to_sql('zipcodes',con = self.engine,if_exists='replace',index=False)


    def get_max_station_index(self):
        self.mysql_connect()
        sql='select max(station_id) as max_stid, max(year) as max_year from stations;'
        conn=self.engine.connect()
        result=conn.execute(text(sql))
        for row in result:
            max_index=row[0]
            max_year=row[1]
        return max_index,max_year

    def get_station_ids(self):
        self.mysql_connect()
        sql="select distinct station, station_id  from stations s ;"
        conn=self.engine.connect()
        df = pd.read_sql(sql, conn)
        conn.close()
        return df



    def load_new_stations(self):
        logger.info("Starting : Load new station details")
        st=time.time()
        ext='csv'
        logger.debug("Get max index from stations table")
        max_index,max_year=self.get_max_station_index()
        stations_ids=self.get_station_ids()

        path=os.path.join("csv",self.config['load']['period'])
        for year in range(self.config['load']['start_year'],self.config['load']['end_year']+1):
            if year > max_year:
                logger.debug(f"Loading year {year}")
                yp=os.path.join(path,str(year))
                if os.path.exists(yp) and os.path.isdir(yp):
                    for file in os.listdir(yp):
                        csv_fp=os.path.join(yp,file)
                        if ext in csv_fp:
                            with open(csv_fp, 'r') as f:
                                rf = csv.reader(f)
                                x = 0
                                for row in rf:
                                    if x == 0:
                                        header = [s.strip('"') for s in row]
                                        dt_idx=header.index('DATE')
                                        st_idx = header.index('STATION')
                                        name_idx = header.index('NAME')
                                        lat_idx = header.index('LATITUDE')
                                        lon_idx = header.index('LONGITUDE')
                                    if x==1:
                                        sd={}
                                        date=row[dt_idx].strip('"')
                                        dto=datetime.datetime.strptime(date,'%Y-%m-%dT%H:%M:%S')
                                        sd['year']=dto.year
                                        sd["station"] = row[st_idx].strip('"')
                                        sd["name"] = row[name_idx].strip('"')
                                        sd["latitude"] = row[lat_idx].strip('"')
                                        sd['longitude']=row[lon_idx].strip('"')
                                        self.station_details.append(sd)
                                    x+=1
                                    if x>1:
                                        break

                df=pd.DataFrame(self.station_details)
                df=df.drop_duplicates()
                df[['name','country_code']]=df['name'].str.split(', ',expand=True)
                df['country_code']=df['country_code'].apply(lambda x: str(x)[-2:])

                logger.debug(f"Station df size {df.shape} ,adding country names")
                c=open('additional_files/country_list.txt','r')
                s=c.read()
                country_list=s.split("\n")
                country_code=[]
                country_name=[]
                n=0
                for item in country_list:
                    if n>1:
                        s=item.replace(' ','')
                        country_code.append(s[0:2])
                        country_name.append(s[2:])
                    n+=1
                
                country_df=pd.DataFrame({'country_code':country_code,'country_name':country_name})
                new_stations=df.merge(country_df,on=['country_code'],how='left')
                new_stations = new_stations.rename(columns={col: col.lower() for col in new_stations.columns})
                col_order=['year','station','name','latitude','longitude','country_code','country_name']
                new_stations=new_stations[col_order]

                new_stations=new_stations.merge(stations_ids,how='left',on=['station'])

                null_values = new_stations['station_id'].isnull()
                null_df = new_stations[null_values].copy()

                if null_values.any():
                    new_stations.loc[null_values, 'station_id'] += (max_index + 1)
                    null_df['station_id'],_ = pd.factorize(null_df['station'])
                    null_df['station_id'] += (max_index + 1)
                
                old_stations=pd.read_csv('stations.csv')
                all_stations=pd.concat([old_stations,null_df],ignore_index=True)
                all_stations.to_csv('new_stations.csv')

                self.mysql_connect()

                logger.debug(f"station_df {new_stations.shape} loading")
                new_stations.to_sql('stations',con = self.engine,if_exists='append',index=False)
                logger.debug(f"Successfully loaded, time taken {st-time.time()}")
            else:
                logger.debug(f"{year} year station details already exists")

if __name__ == '__main__':
    ld=loader()
    # ld.download_files()

    ld.load_weather_data()
    print("Loading complete")

    


        
        

    










    



    







    
        






