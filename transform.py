def get_transformed_data(path_to_data:str)->pd.DataFrame:
    select_cols = ['coord_lon', 'coord_lat', 'weather_0_main', 'weather_0_description', 'main_temp','main_feels_like','main_temp_min','main_temp_max'\
            ,'main_pressure','main_humidity','main_sea_level','main_grnd_level','visibility','wind_speed','wind_deg','wind_gust','clouds_all'\
            , 'dt','sys_country','sys_sunrise','sys_sunset','timezone','name']
    unix_time_cols = ['dt','sys_sunrise','sys_sunset']
    local_time_cols = ['dt_local','sys_sunrise_local','sys_sunset_local']
    col_rep_shift_utc = ['timezone']
    df = pd.read_csv(path_to_data, usecols= select_cols, delimiter= '|')
    df.drop_duplicates(subset = ['dt'], keep= 'last', inplace= True, ignore_index= True)
    #local time
    for i in range(0,3):
        df[local_time_cols[i]] = df[unix_time_cols[i]] + df[col_rep_shift_utc[0]]
        df[local_time_cols[i]] = pd.to_datetime(df[local_time_cols[i]], unit= 's')
    #UTC time
    for c in unix_time_cols:
        df[c+"_utc"] = pd.to_datetime(df[c], unit= 's')
    df['state'] = (path_to_data.split('weather_api_response_files')[1]).split(os.sep)[2]
    # df['etl_timestamp'] = pd.Timestamp.now()
    # df['location'] = (((path_to_data.split('weather_api_response_files')[1]).split(os.sep)[3]).split("_")[2]).split(".")[0]
    df.drop(columns= unix_time_cols, axis=1, inplace=True)
    df.drop(columns= ['timezone'], axis=1, inplace=True)
    return df
