from pprint import pprint 
import copy

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

if __name__ == "__main__":
    data = {'coord': {'lon': 42.2781, 'lat': 42.2781}, 'weather': [{'id': 804, 'main': 'Clouds', 'description': 'overcast clouds', 'icon': '04d'}], 'base': 'stations', 'main': {'temp': 295.66, 'feels_like': 295.85, 'temp_min': 295.66, 'temp_max': 295.66, 'pressure': 1017, 'humidity': 72, 'sea_level': 1017, 'grnd_level': 1011}, 'visibility': 10000, 'wind': {'speed': 0.56, 'deg': 324, 'gust': 1.36}, 'clouds': {'all': 96}, 'dt': 1748762606, 'sys': {'country': 'GE', 'sunrise': 1748741829, 'sunset': 1748796037}, 'timezone': 14400, 'id': 616022, 'name': 'Abasha', 'cod': 200}
    print(flatten_nested_dict(oGdata= data))