import datetime
import pandas as pd
from graph_utils import timedelta


def create_df_path(path, end_name):
    # Creates a daframe from a list of edges
    
    for index in reversed(range(len(path))):

        # Add walking departure and arrival time. Skip if it ends it a walk.
        if pd.isnull(path[index]["to_arrival_time"]) and index != 0: 
            path[index]["from_departure_time"] = (datetime.datetime.min + timedelta(path[index - 1]["from_departure_time"]) - datetime.timedelta(seconds = float(path[index]['travel_time']) // 1)).time()
            path[index]["to_arrival_time"] = path[index - 1]["from_departure_time"]
            path[index]["to_stop_name"] = path[index - 1]["from_stop_name"]

        # Fix for a transport that ends with a walk
        if pd.isnull(path[index]["to_arrival_time"]) and index == 0:
            path[index]["from_departure_time"] = path[index + 1]["to_arrival_time"]
            path[index]["to_arrival_time"] = (datetime.datetime.min + timedelta(path[index + 1]["to_arrival_time"]) + datetime.timedelta(seconds = float(path[index]['travel_time']) // 1)).time()
            path[index]["from_stop_name"] = path[index + 1]["to_stop_name"]
            path[index]["to_stop_name"] = end_name

    return pd.DataFrame(list(path))


def aggregate_path(df_path):
    # Aggregates the datafram of a path, by aggregating trips that take the same mode of transport (aggregating on trip_id)
    
    mask = df_path['trip_id'].isna()
    df_path.loc[mask, 'trip_id'] = df_path[['from_stop_id','to_stop_id']].apply(lambda x : '{}-{}'.format(x[0],x[1]), axis=1)[mask]
    return (df_path.sort_values('path_index', ascending=False)
            .groupby('trip_id', dropna=False)
            .agg({'route_id': 'first',

                  'from_stop_id': 'first',
                  'from_stop_name': 'first',
                  'from_departure_time': 'first',

                  'from_stop_lat': 'first',
                  'from_stop_lon': 'first',

                  'to_stop_id': 'last',
                  'to_stop_name': 'last', 
                  'to_arrival_time': 'last',

                  'transfer_type': 'first',
                  'travel_time': 'sum',
                  'path_index': 'first',
                  })
            .sort_values('path_index', ascending=False)
            .reset_index())

def get_trip_stops(df, df_agg):
     # Create a map from path_index to a list of stops that belongs to this aggregation

    path_stop_map = {}

    index = 0
    agg_stops = list(reversed(df_agg["path_index"]))

    for value in range(1,agg_stops[-1] + 1):
        
        stop_dict = df.loc[df["path_index"] == value].to_dict('records')[0]
        
        if agg_stops[index] not in path_stop_map:
            path_stop_map[agg_stops[index]] = [stop_dict]

        elif value <= agg_stops[index]:
                path_stop_map[agg_stops[index]].append(stop_dict)

        if value == agg_stops[index]:
            index += 1
    
    return path_stop_map



def get_subtrip_string(path_index, path_stop_map):
    # helper funtion for get_strip_trip
    sub_stops = path_stop_map[path_index]
    if len(sub_stops) <= 1:
        return ""
    return "".join([f"""<div style="margin-left: 16px"> {sub_stop["from_departure_time"].strftime("%H:%M")} {sub_stop["from_stop_name"]} </div>""" for sub_stop in reversed(sub_stops[:-1])])

transfer_types = {"0": "", "1": "(Transfer)", "2": "(Walk)"}

def get_trip_string(trip, path_stop_map):
    # helper funtion for get_html
    return f"""
            <section style="display flex;flex-direction: column; font-size: 13px">
                <div onclick="this.parentNode.childNodes[3].style.display == 'block' ?
                                       this.parentNode.childNodes[3].style = 'display:none' :
                                       this.parentNode.childNodes[3].style = 'display:block';"{" style='cursor: pointer;'" if len(path_stop_map[trip["path_index"]]) > 1 else ''}>
                    <span>
                        {trip["from_departure_time"].strftime("%H:%M")}
                    </span> 
                    {trip["from_stop_name"]}  {transfer_types[trip["transfer_type"]]}
                </div>
                <section style='display:none;'>
                   {get_subtrip_string(trip["path_index"], path_stop_map)}
                </section>
                <div>
                    <span>
                        {trip["to_arrival_time"].strftime("%H:%M")}
                    </span> 
                    {trip["to_stop_name"]}
                </div>
            </section>
            """

def get_html(df, df_agg, q):
    # return the html code for the visualziation of the paths.
    
    start_station = df_agg.iloc[0]
    end_station = df_agg.iloc[-1]
    
    path_stop_map = get_trip_stops(df, df_agg)
    
    trip_strings = "<div style='margin-left: 12px;'>🠗</div>".join([get_trip_string(row, path_stop_map) for index, row in df_agg.iterrows()])

    transport_string = f"""<div style="margin-bottom: 16px;">
                            <span onclick="this.parentNode.childNodes[7].style.display == 'block' ?
                                           this.parentNode.childNodes[7].style = 'display:none' :
                                           this.parentNode.childNodes[7].style = 'display:block'"
                                style="margin-right: 16px;font-weight:bold;cursor: pointer;">{start_station["from_departure_time"].strftime("%H:%M")}-{end_station["to_arrival_time"].strftime("%H:%M")}</span>
                            <span>{(timedelta(end_station["to_arrival_time"]) - timedelta(start_station["from_departure_time"])).seconds // 60} min</span> <span style="margin-left: 18px"><i>Q {(q * 100):.0f}% Success rate</i></span>
                            <section style="display: none;">
                            {trip_strings}
                            </section>
                        </div>"""

    return transport_string