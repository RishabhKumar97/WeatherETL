import requests
import re
import pandas as pd
import re
import copy
import configparser
import os

#Airflow imports
from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

def flatten_nested_dict(oGdata: dict) -> dict:
    output = {}
    data = copy.deepcopy(oGdata)
    flag = True
    while flag:
        temp = {}
        flag = False  # Assume no more nesting; prove otherwise
        for k in data:
            if isinstance(data[k], dict):
                for key_in in data[k]:
                    temp[k + "_" + key_in] = data[k][key_in]
                    if isinstance(data[k][key_in], (dict, list)):
                        flag = True
            elif isinstance(data[k], list):
                for i, item in enumerate(data[k]):
                    temp[f"{k}_{i}"] = item
                    if isinstance(item, (dict, list)):
                        flag = True
            else:
                temp[k] = data[k]
        data = copy.deepcopy(temp)
        output = data

    return output

#get geo codes for locations through api
def get_codes_for_location(location:list|str)->pd.DataFrame:
    API_KEY = os.environ.get('AIRFLOW_VAR_MY_API_KEY')
    geo_codes_location_dict = {
        'name': [],
        'lat': [],
        'lon': [],
        'country': [],
        'state': []
    }
    if(type(location) == str):
        location = [location]
    for each in location: 
        geo_coding_api_url = f"http://api.openweathermap.org/geo/1.0/direct?q={each}&limit=1&appid={API_KEY}"

        geo_code_response = requests.get(geo_coding_api_url).content

        #manual decoding for byte literal response and converting it into a dict .decode didn't work
        new_geo_code_response = eval(re.sub("^b[']|[']$", "", str(geo_code_response).replace(r"\x", "")))
        geo_codes_location_dict['name'].append(new_geo_code_response[0]['name'])
        geo_codes_location_dict['lat'].append(new_geo_code_response[0]['lat'])
        geo_codes_location_dict['lon'].append(new_geo_code_response[0]['lon'])
        geo_codes_location_dict['country'].append(new_geo_code_response[0]['country'])
        geo_codes_location_dict['state'].append(new_geo_code_response[0]['state'])
    
    geo_codes_df = pd.DataFrame(geo_codes_location_dict)
    return geo_codes_df

def get_weather_data_and_write_to_csv(geo_codes_df: pd.DataFrame,path_to_response_file_directory):
    API_KEY = os.environ.get('AIRFLOW_VAR_MY_API_KEY')
    select_cols = ['coord_lon', 'coord_lat', 'weather_0_main', 'weather_0_description', 'main_temp','main_feels_like','main_temp_min','main_temp_max'\
            ,'main_pressure','main_humidity','main_sea_level','main_grnd_level','visibility','wind_speed','wind_deg','wind_gust','clouds_all'\
            , 'dt','sys_country','sys_sunrise','sys_sunset','timezone','name']
    path_to_data =[]
    #ensure base path exists
    os.makedirs(path_to_response_file_directory, exist_ok= True)
    for each in geo_codes_df.itertuples():
        lon = each.lon
        lat = each.lat
        name = each.name
        country = each.country
        state = each.state
        # weather data api url
        weather_data_api =f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}"
        #decode and store the response
        weather_data_api_response = eval(requests.get(weather_data_api).content.decode('utf-8'))
        # flatten and create df
        df_weather_data = pd.DataFrame(flatten_nested_dict(weather_data_api_response), index= [0])
        df_weather_data_select_cols = df_weather_data[select_cols]
        # print(df_weather_data.dtypes)
        #ensure file path exists for writing the df
        file_path = f"{country}{os.sep}{state}{os.sep}"
        full_path = os.path.join(path_to_response_file_directory, file_path)
        os.makedirs(full_path, exist_ok= True)
        # save the df into a file
        print(f"Inserting raw data to path: {full_path}", f"file name: weather_data_{name.lower()}.csv")
        # Write to CSV, include header only if the file doesn't exist
        df_weather_data_select_cols.to_csv(f'{full_path}weather_data_{name.lower()}.csv', sep= '|', index= False, mode = 'a', header= not(os.path.isfile(f'{full_path}weather_data_{name.lower()}.csv')))
        path_to_data.append(f'{full_path}weather_data_{name.lower()}.csv')
            
@dag
def weather_etl_dag():
    @task
    def get_geo_codes():
        cfg_ini_file_path = f'config{os.sep}cfg.ini'
        print("cfg_ini_file_path: ",  cfg_ini_file_path)
        config = configparser.ConfigParser(allow_no_value=True)
        config.read(cfg_ini_file_path)
        locations = config.options('Locations')
        print(locations)
        geo_codes_df = get_codes_for_location(locations)
        return geo_codes_df
    
    @task
    def write_weather_data_for_geo_codes_to_csv(geo_codes_df):
        path_to_response_file_directory = f"weather_api_response_files{os.sep}"
        print("path_to_response_file_directory: ", path_to_response_file_directory)
        get_weather_data_and_write_to_csv(geo_codes_df, path_to_response_file_directory)
    
    
    write_weather_data_for_geo_codes_to_csv(get_geo_codes())
 

weather_etl_dag()