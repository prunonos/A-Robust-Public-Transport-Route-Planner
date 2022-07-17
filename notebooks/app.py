# +
# it may be possible that we have to install some packages even though they are present in the environment.yml
# # !pip install networkx pyarrow --quiet

# +
import os
import random
import datetime
import numpy as np
import pandas as pd
import networkx as nx

import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go

import ipywidgets as widgets
from IPython.core.display import display, HTML


from hdfs3 import HDFileSystem
from graph_utils import find_path, timedelta
from app_utils import get_html, aggregate_path, create_df_path

TIME_FORMAT = '%H:%M:%S'
# -

# %load_ext autoreload
# %autoreload 2
# %load_ext sparkmagic.magics 

# +
# start spark application
username = os.environ['RENKU_USERNAME']
server = "http://iccluster029.iccluster.epfl.ch:8998"

# set the application name as "<your_gaspar_id>-homework3"
get_ipython().run_cell_magic(
    'spark',
    line='config', 
    cell="""{{ "name": "{0}-final-assignment", "executorMemory": "4G", "executorCores": 4, "numExecutors": 10, "driverMemory": "4G"}}""".format(username)
)
get_ipython().run_line_magic(
    "spark", "add -s {0}-final-assignment -l python -u {1} -k".format(username, server)
);

# + language="spark"
# from io import StringIO
# from datetime import datetime
# import pandas as pd
#
# # import delays file
# delays = spark.read.orc('/group/data-science-group/df_delays')
# delays = delays.dropna().cache()

# + language="spark"
#
# # function to get switchtimes and walking times of the route
# def getSwitchTimeTable(df):
#     walking_times = {stopId: 0 for stopId in df.from_stop_id[1:]}
#     # walking time must be selected also from the table, but in this sample we didnt had the walking_time
#     switch_times = {}
#     hours = []
#
#     arrival_time = datetime.strptime(df.iloc[0].to_arrival_time,'%H:%M:%S')
#     for i in range(1,len(df)):
#         hours.append(df.iloc[i].from_departure_time[:2])
#         departure_time = datetime.strptime(df.iloc[i].from_departure_time,'%H:%M:%S')
#         switch_times[df.iloc[i].from_stop_id] = (departure_time - arrival_time).seconds
#         arrival_time = datetime.strptime(df.iloc[i].to_arrival_time,'%H:%M:%S')
#             
#     df = pd.DataFrame({'bpuic':switch_times.keys(), 'hour': map(int, hours), 'switch_time':switch_times.values(), 'walking_time':walking_times.values()})
#     sparkDF = spark.createDataFrame(df)    
#     return sparkDF

# + language="spark"
# import pyspark.sql.functions as F
# import numpy as np
#
# def get_Qroute(df):
#     sparkDF = getSwitchTimeTable(df)
#     # now join the switch times and walking times with the delays (we keep only the ones we need) 
#     join_delay_switch_DF = sparkDF.join(delays,['bpuic','hour'],how='inner') # delays -> already imported
#     ratioDF = join_delay_switch_DF.withColumn('ratio', ((F.col('walking_time') + F.col('delay')) < F.col('switch_time')).cast('integer'))
#     q_values = ratioDF.groupBy(['bpuic','hour']).agg({'ratio':'mean'}) #.collect() # q_values[i] = [0,1]    Qroute = F.product(q_values)
#     return q_values
# -

hdfs = HDFileSystem(user='eric')
def load_df(path):
    return pd.concat([pd.read_orc(hdfs.open(f)) for f in hdfs.glob(path) if not 'SUCCESS' in f])


df_transport = load_df('/group/data-science-group/df_transport')
df_transport['transfer_type'] = '0'

df_walking = load_df('/group/data-science-group/df_transfer_with_walking_transfers')
df_walking = df_walking.rename(columns={'min_transfer_time': 'travel_time'})

df = pd.concat([df_transport, df_walking])
df['from_departure_time'] = pd.to_datetime(df['from_departure_time'], format=TIME_FORMAT).dt.time
df['to_arrival_time'] = pd.to_datetime(df['to_arrival_time'], format=TIME_FORMAT).dt.time

G = nx.from_pandas_edgelist(df, 
                            source='from_stop_id', 
                            target='to_stop_id', 
                            edge_attr=True,
                            create_using=nx.MultiDiGraph)

df_stops = df.drop_duplicates(subset='from_stop_id', keep='first')[['from_stop_id','from_stop_name']]

df_stops_dict = {f"{name} - {id}": id for id, name in zip(df_stops['from_stop_id'], df_stops['from_stop_name'])}

# # A robust public transport route planner

# +
hoverinfo = 'name+text'
fig = go.Figure(go.Scattermapbox(
    mode = "markers+lines",
    lon = [],
    lat = [],
    hovertext=[],
    hoverinfo=hoverinfo,
    name='Stop',
    marker = {'size': 10, 'color':'#1f77b4'},
    showlegend=False
    ))
fig.add_trace(
    go.Scattermapbox(
        mode='markers',
        lon=[],
        lat=[],
        hovertext=[],
        hoverinfo=hoverinfo,
        name='Connection',
        marker = {'size': 15, 'color':'#1f77b4'},
    showlegend=False))
fig.update_layout(
    margin ={'l':0,'t':0,'b':0,'r':0},
    mapbox = {
        'center': {'lon': 8.540192, 'lat': 47.378177},
        'style': "stamen-terrain",
        'zoom': 10})

stream_fig = go.FigureWidget(fig)
stream_fig
# -

start = widgets.Combobox(
    placeholder='Choose departure station',
    options=list(df_stops_dict.keys()),
    description='From:',
    ensure_option=True,
    disabled=False
)
start

end = widgets.Combobox(
    placeholder='Choose arrival station',
    options=list(df_stops_dict.keys()),
    description='End:',
    ensure_option=True,
    disabled=False
)
end

hour = widgets.Dropdown(
    options=list(range(8, 17)),
    value=15,
    description='Hour:',
    disabled=False,
)
hour

minute = widgets.Dropdown(
    options=list(range(0, 60, 5)),
    value=35,
    description='Minutes:',
    disabled=False,
)
minute

# +
button = widgets.Button(description="Plan my route")
output = widgets.Output()

display(button, output)

def on_button_clicked(button):
    if start.value == '' or end.value == '' or hour.value == '' or minute.value == '':
        with output:
            print('Give values')
        return
    output.clear_output()
    
    with output:
        display(HTML("""<div style="font-size: 32px; margin-bottom: 32px;"><span>{}</span> → <span>{}</span></div>""".format(start.value.split("-")[0], end.value.split("-")[0])))
    
    count = 0
    i = 1
    prev_q = 0

    while count < 5 and i < 18 and prev_q < 0.95:
        try:
            start_time, path = find_path(G, df_stops_dict[start.value], df_stops_dict[end.value], f'{hour.value:02}:{minute.value:02}:00', minimum_wait_time=f'00:0{i//2}:{(i % 2) * 30}')
        except nx.NetworkXNoPath as e:
            with output:
                print(e)
            return
        
        df_path = create_df_path(path, end.value.split("-")[0])
        df_path_agg = aggregate_path(df_path)
        
        df_path_agg_string = df_path_agg.to_csv()
        get_ipython().run_cell_magic('spark', '', f"string = u'''{df_path_agg_string}'''\nsample_route_str = StringIO(string)\nsample_route = pd.read_csv(sample_route_str, index_col='trip_id')")
        get_ipython().run_cell_magic('spark', '-o Q', 'Q = get_Qroute(sample_route)')
        
        q_values_df = pd.DataFrame(Q,columns=['bpuic','hour','avg(ratio)'])
        Qroute = np.prod(q_values_df["avg(ratio)"])  # it could be also the mean
        if Qroute > prev_q:
            count += 1
            with stream_fig.batch_update():
                stream_fig.data[0].lat = df_path.from_stop_lat  
                stream_fig.data[0].lon = df_path.from_stop_lon
                stream_fig.data[0].hovertext = df_path.from_stop_name 

                stream_fig.data[1].lat = df_path_agg.from_stop_lat.to_list() + df_path.from_stop_lat.to_list()[-1:]
                stream_fig.data[1].lon = df_path_agg.from_stop_lon.to_list() + df_path.from_stop_lon.to_list()[-1:]
                stream_fig.data[1].hovertext = df_path_agg.from_stop_name.to_list() + df_path.from_stop_name.to_list()[-1:]



            html = get_html(df_path, df_path_agg, Qroute)
            with output:
                display(HTML(html))
                
            prev_q = Qroute
        
        i += 1


button.on_click(on_button_clicked)
