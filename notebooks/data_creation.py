# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.8
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# ## Setup

# %%
# %load_ext sparkmagic.magics

# %%
import os
username = os.environ['RENKU_USERNAME']
server = 'http://iccluster029.iccluster.epfl.ch:8998'
from IPython import get_ipython
get_ipython().run_cell_magic('spark', line="config", 
                    cell=f"""{{ "name":"{username}-final-assignment",
                               "executorMemory":"4G",
                               "executorCores":4,
                               "numExecutors":10 }}""")

# %%
get_ipython().run_line_magic("spark", f"add -s {username}-final-assignment -l python -u {server} -k")

# %% [markdown]
# Here is a [cookbook](https://opentransportdata.swiss/wp-content/uploads/2016/11/gtfs_static.png) from how the data is related.

# %% [markdown]
# ## Stops
#
# `'/data/sbb/orc/allstops'`

# %% language="spark"
# import math
# import pyspark.sql.functions as F
#
#
# def non_udf_distance((lat1, lon1), (lat2, lon2)):
#     R = 6371
#     phi1 = math.radians(lat1)
#     phi2 = math.radians(lat2)
#     delta_phi = phi1-phi2
#     delta_lambda = math.radians(lon2-lon1)
#
#     a = math.sin(delta_phi/2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda/2) ** 2;
#     c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
#     return R * c
#
# @F.udf
# def zurich_distance(lat1, lon1):
#     return non_udf_distance((lat1, lon1), (47.378177, 8.540192))
#
# @F.udf
# def distance(lat1, lon1, lat2, lon2):
#     return non_udf_distance((lat1, lon1), (lat2, lon2))

# %% language="spark"
# df_stops = spark.read.orc('/data/sbb/orc/allstops')

# %% [markdown]
# We add a column with the distance to Zurich and only keep the stops that are in a 15km radius of Zürich's train station, `Zürich HB (8503000)`, `(lat, lon) = (47.378177, 8.540192)`. <br>
# We go from having 46689 stops to 2122 stops.

# %% language="spark"
# df_stops = df_stops.withColumn("zurich_distance", zurich_distance(F.col("stop_lat"), F.col("stop_lon")).alias("distance"))
# df_stops = df_stops.filter(F.col("zurich_distance") < 15)

# %% [markdown]
# ## Stop times
#
# `'/data/sbb/csv/stops_times'`

# %% language="spark"
# df_stop_times = spark.read.csv("/data/sbb/csv/stop_times/2019/05/01", header=True)

# %% [markdown]
# Remove all times that are outisde of normal business hours from the stop times. <br>
# We consider busniess hous to be from 8:00 to 17:00. <br>
# We go from having 9'999'930 stop times to 5'327'267 stop times.

# %% language="spark"
# df_stop_times = df_stop_times.filter((F.col('arrival_time') > '08:00:00') & (F.col('departure_time') < '17:00:00'))

# %% [markdown]
# We join the filtered stops with the filtered stop times. <br>
# We have a total of 905'727 rows.

# %% language="spark"
# df_stops_joined = df_stops.join(df_stop_times, "stop_id", "inner")

# %% [markdown]
# ## Trips
#
# `'/data/sbb/csv/trips'`
#
# We have 954'278 trips

# %% language="spark"
# df_trips = spark.read.csv("/data/sbb/csv/trips/2019/05/01", header=True)
# df_trips.printSchema()

# %% [markdown]
# ## Calendar
#
# `'/data/sbb/csv/calendar'`

# %% language="spark"
# df_calendar = spark.read.csv('/data/sbb/csv/calendar/2019/05/01', header=True)

# %% [markdown]
# We filter to only include wednesday. <br> 
# By doing this we go from having 20350 to having 11943 rows. <br>

# %% language="spark"
# df_calendar_single_day = df_calendar.filter(F.col('wednesday') == 1).select('service_id')

# %% [markdown]
# Number of trips for when only includuing wednesdays is 590608.

# %% language="spark"
# df_trips_single_day = df_trips.join(df_calendar_single_day, 'service_id', 'inner')

# %% [markdown]
# Number of stops for when only includuing wednesdays is 574059.

# %% language="spark"
# df_stops_single_day = df_stops_joined.join(df_trips_single_day, 'trip_id', 'inner')

# %% [markdown]
# ## Create edges

# %% [markdown]
# We first drop the columns that we will not need in the graph

# %% language="spark"
# df_stops_single_day_pruned = df_stops_single_day.drop('location_type', 'location_type', 'zurich_distance', 'pickup_type', 'drop_off_type', 'service_id', 'trip_headsign', 'trip_short_name', 'direction_id')

# %% [markdown]
# Here we join the dataframe with itself with the next entry in the `stop_sequence` and first rename all columns in the from and to variant of the df accordingly

# %% language="spark"
# right = df_stops_single_day_pruned.select([F.col(c).alias("from_"+c) for c in df_stops_single_day_pruned.columns])
# left = df_stops_single_day_pruned.select([F.col(c).alias("to_"+c) for c in df_stops_single_day_pruned.columns])
# df_from_to = right.join(left, [right.from_stop_sequence == left.to_stop_sequence-1, right.from_trip_id == left.to_trip_id], 'inner')

# %% [markdown]
# We precalculate the travel time.

# %% language="spark"
# df_from_to = df_from_to.withColumn('travel_time', F.unix_timestamp(F.to_timestamp('to_arrival_time')) - F.unix_timestamp(F.to_timestamp('from_departure_time')))

# %% language="spark"
# df_from_to_pruned = df_from_to.select(['from_trip_id', 'from_stop_id', 'from_stop_name', 'from_parent_station', 'from_stop_lat', 'from_stop_lon', 'from_departure_time', 'from_route_id', 'to_stop_id', 'to_stop_name', 'to_parent_station', 'to_arrival_time', 'travel_time'])

# %% [markdown]
# Removing the from and to prefixes from certain column names. <br>
# size = 518_029 <br>
# number of distinct stop ids = 1691 <br>
# number of distinct parent stations = 85 (multiple children) + 1458 (one child) = 1548 <br>

# %% language="spark"
# df_from_to_pruned = df_from_to_pruned.withColumnRenamed("from_trip_id", "trip_id")
# df_from_to_pruned = df_from_to_pruned.withColumnRenamed("from_route_id", "route_id")

# %% language="spark"
# df_from_to_pruned.write.mode('overwrite').orc('/group/data-science-group/df_transport')

# %% [markdown]
# ## Transfers
#
# `'/data/sbb/csv/transfers'`

# %% language="spark"
# df_transfer = spark.read.csv('/data/sbb/csv/transfers/2019/05/01', header=True)

# %% [markdown]
# # Get stations that are less than 500 meters away

# %% language="spark"
# df_stops.count() 


# %% [markdown]
# ### Create nearby_stops

# %% tags=[] language="spark"
# right = df_stops.select([F.col(c).alias("from_"+c) for c in df_stops.columns])
# left = df_stops.select([F.col(c).alias("to_"+c) for c in df_stops.columns])
#
# crossed_df_stops = right.crossJoin(
#     left
# ).select("from_stop_id", "from_stop_name", "from_stop_lat", "from_stop_lon", "from_parent_station", 
#          "to_stop_id", "to_stop_lat", "to_stop_lon", "to_parent_station")
#
#
#
#
#
#
# crossed_df_stops = crossed_df_stops.withColumn("pairwise_distance", distance(F.col("from_stop_lat"), F.col("from_stop_lon"), F.col("to_stop_lat"), F.col("to_stop_lon")).alias("distance"))
#
#
#
# nearby_stops = crossed_df_stops.filter(F.col("pairwise_distance") < 0.5)\
#                                 .filter(F.col("from_stop_id") != F.col("to_stop_id"))\
#                                 .filter((F.col('from_parent_station') != F.col('to_parent_station'))\
#                                           | (F.col('from_parent_station') == '')\
#                                           | (F.col('to_parent_station') == ''))
#
# nearby_stops.write.mode('overwrite').orc('/group/data-science-group/nearby_stops')

# %% [markdown]
# Nearby stops has 15722 rows

# %% [markdown]
# ### Create df_transfer_with_walking_transfers

# %% [markdown]
# Seems to also include transfers between stations with different parent stations: <br>
# Example: 8502221:0:2|8573718:0:C

# %% language="spark"
# df_transfer_zurich = df_transfer.join(df_stops.withColumnRenamed("stop_id", "from_stop_id"), on="from_stop_id")

# %% [markdown]
# We're assuming a 50 metres per minut

# %% language="spark"
# from pyspark.sql.functions import lit
# nearby_stops_selected = nearby_stops.withColumnRenamed("stop_id", "from_stop_id")\
#                             .withColumn("transfer_type", lit('1'))\
#                             .withColumn("min_transfer_time", F.col('pairwise_distance')*1000/50*60 + 120)\ 
#                             .select("from_stop_id", "to_stop_id", "from_stop_name", "from_stop_lat", "from_stop_lon", "transfer_type", "min_transfer_time")
#
# df_transfer_zurich_selected = df_transfer_zurich.withColumnRenamed("stop_name", "from_stop_name")\
#                                                 .withColumnRenamed("stop_lat", "from_stop_lat")\
#                                                 .withColumnRenamed("stop_lon", "from_stop_lon")\
#                                                 .select("from_stop_id", "to_stop_id", "from_stop_name", "from_stop_lat", "from_stop_lon", "transfer_type", "min_transfer_time")
#
# df_transfer_with_walking_transfers = df_transfer_zurich_selected.union(nearby_stops_selected)
#
# df_transfer_with_walking_transfers.write.mode('overwrite').orc('/group/data-science-group/df_transfer_with_walking_transfers')

# %% [markdown]
# ---

# %% [markdown]
# ## Initialization and Setup for delays

# %% language="spark"
# print('We are using Spark %s' % spark.version)

# %% language="spark"
# istdaten = spark.read.orc('/data/sbb/part_orc/istdaten')

# %% [markdown]
# This dataframe contains 2 104 333 948 records

# %% [markdown]
# ---

# %% [markdown]
# ### FILTER BY STATIONS AND HOUR

# %% [markdown]
# We are working only in the Zurich Area. These stations are contained in a table saved in HDFS.

# %% language="spark"
# df_stops = spark.read.orc('/group/data-science-group/df_stops_filtered_on_zurich_distance/')

# %% [markdown]
# We have 2122 stops close to Zurich

# %% language="spark"
# all_stations = df_stops.select('stop_id')

# %% [markdown]
# We need to have all the stations in the same format because in istdaten all station ids are integers.

# %% language="spark"
# import pyspark.sql.functions as F
# all_stations = all_stations.withColumn('stop_id', F.regexp_replace(all_stations.stop_id,'(Parent)',''))
# all_stations = all_stations.withColumn('bpuic', F.regexp_replace(all_stations.stop_id,'[^\d]+(.)*',''))
# all_stations = all_stations.select('bpuic').distinct()    # only take the distinct values of stations

# %% [markdown]
# We join all_stations and istdaten, in order to keep only the stops in Zurich area. We see that we go from  2 104 333 948 to 448 613 610, it gets divided by 5.

# %% language="spark"
# data_stations = all_stations.join(istdaten, 'bpuic', 'inner')

# %% [markdown]
# We reduce the amount of stops considerably. Now, we will filter by the hour of stop.

# %% [markdown]
# We filter to take only the stops where there is an arrival time value.

# %% language="spark"
# data_stations = data_stations.filter(data_stations.ankunftszeit != '')

# %% language="spark"
# ##Helper function used for strings
#
# @F.udf
# def getHour(x):
#     return int(x[11:13])

# %% language="spark"
# data_stations = data_stations.withColumn('hour', getHour(data_stations.ankunftszeit))

# %% [markdown]
# Now we can filter the stops within the range of time [8,17] in order to keep only the business hours.

# %% language="spark"
# aux_filtered = data_stations.filter((F.col('year') == 2018) & (F.col('hour') >= 7)).cache()
# data_filtered = aux_filtered.filter(F.col('hour') <= 17)

# %% [markdown]
# Now, we can compute the delays of each station at every hour.

# %% language="spark"
# data_filtered = data_filtered.withColumn('ankunftszeit', F.unix_timestamp(F.col('ankunftszeit'), 'dd.MM.yyyy HH:mm'))
# data_filtered = data_filtered.withColumn('an_prognose', F.unix_timestamp(F.col('an_prognose'), 'dd.MM.yyyy HH:mm:ss'))

# %% [markdown]
# ---

# %% [markdown]
# ## DELAY COMPUTATION

# %% [markdown]
# The delays are computed based on the 2018 data. \
# We use the following logic : 
# " IF Delay > Switch_time - Delay - Walking_time THEN Connection_Missed"

# %% language="spark"
# data_delays = data_filtered.withColumn('delay', F.col('an_prognose') - F.col('ankunftszeit'))

# %% language="spark"
# delays = data_delays.select(['bpuic','hour','delay'])

# %% [markdown]
# The delays have to be stored in HDFS because we will used them in the Q-value computation as precomputed values

# %% language="spark"
# ##Allows to save the delays in HDFS
# delays.write.mode('overwrite').save('/group/data-science-group/df_delays', 'orc')

# %% [markdown]
# ---
