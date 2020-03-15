import pandas as pd
import numpy as np
from collections import defaultdict, Counter
from shapely.geometry import Point
from shapely.geometry import LineString
from shapely import wkt
from datetime import datetime, timedelta

import json


def get_trips_obj(df):
    trips = defaultdict(lambda: [[], []])  # key: line, value: list of 2 lists of dfs (one for each direction)
    for attrs, trip_df in df.groupby(["line_id", "direction_id", "trip_id"]):  # trip_id is the full identifier !!
        (line_id, direction_id, trip_id) = attrs
        trips[line_id][direction_id].append(trip_df)
    return trips


def get_stop_to_lines_obj(df):
    trips = get_trips_obj(df)
    obj = defaultdict(lambda: [])
    for line, directions_list in trips.items():
        for direction_dfs in directions_list:
            for trip_df in direction_dfs:
                add_to_obj(obj, trip_df)
    return obj

def add_to_obj(obj, trip_df):
    for i, row in trip_df.iterrows():
        stop_id = row["stop_id"]
        trip_id = row["trip_id"]
        dist_traveled = row["dist_traveled"]
        obj[stop_id].append((trip_id, dist_traveled))


if __name__ == '__main__':
    df = pd.read_csv("..//Bus_Routes//clean_trips_full.csv")
    obj = get_stop_to_lines_obj(df)
    with open('stops_to_lines.json', 'w') as fp:
        json.dump(obj, fp)
    # loading
    # with open('stops_to_lines.json', 'r') as fp:
    #     obj = json.load(fp)
    print(obj)
