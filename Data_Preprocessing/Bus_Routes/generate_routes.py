import pandas as pd
import numpy as np
from collections import defaultdict
from pyproj import Proj,transform, Transformer
from shapely.geometry import Point
NUM_DIFFERENT_STOPS_ALLOWED = 3
TRANSFORMER = Transformer.from_crs('EPSG:4326', 'EPSG:2157', always_xy=True)


def transform_coordinates(longitude,latitude):
    # transforms long, latt to Irish coordinate points.
    # trans = transform(Proj(init='EPSG:4326'), Proj(init='EPSG:2157'), longitude, latitude)  # longitude, latitude
    trans = TRANSFORMER.transform(longitude, latitude)
    return Point(trans[0],trans[1])


def clean_route(route_df):
    route_df = route_df.sort_values(["stop_sequence"]).reset_index()
    route_df["stop_sequence"] = range(1,route_df.shape[0]+1)
    route_df.rename(columns={"shape_dist_traveled":"dist_traveled","route_short_name":"line_id"},inplace=True)
    route_df = route_df[
        ['trip_id', 'line_id', 'direction_id', 'stop_sequence', "stop_id", 'stop_lat', 'stop_lon',
         'dist_traveled']]
    route_df["stop_point"] = route_df.apply(
        lambda row: transform_coordinates(row["stop_lon"],row["stop_lat"]),axis=1)
    return route_df


def is_new_trip(trip_list,trip_df):
    for existing_trip in trip_list:
        if np.array_equal(existing_trip["stop_id"].values , trip_df["stop_id"].values):
            return False
    return True


def get_all_trips(df):
    line_routes_dic = defaultdict(lambda: [[], []])
    for attrs, trip_df in df.groupby(
            ["route_id", "route_short_name", "direction_id", "trip_id"]):  # trip_id is the full identifier !!
        (route_id, line, direction, trip_id) = attrs
        clean_trip_df = clean_route(trip_df)
        if is_new_trip(line_routes_dic[line][direction], clean_trip_df):
            line_routes_dic[line][direction].append(clean_trip_df)
    return line_routes_dic


def save_trip_file(line_routes_dic):
    df_list = []
    for line, direction_list in line_routes_dic.items():
        df_list += direction_list[0] + direction_list[1]
    all_trips = pd.concat(df_list,ignore_index=True)
    all_trips.to_csv("clean_trips_full.csv",index=False)


if __name__ == '__main__':
    df_stops = pd.read_csv("..//..//Data//Google_transit//stops.txt")
    df_stop_times = pd.read_csv("..//..//Data//Google_transit//stop_times.txt")
    df_trips = pd.read_csv("..//..//Data//Google_transit//trips.txt")
    df_routes = pd.read_csv("..//..//Data//Google_transit//routes.txt")
    df = pd.merge(left=df_stops, right=df_stop_times, left_on='stop_id', right_on='stop_id')
    df = pd.merge(left=df, right=df_trips, left_on='trip_id', right_on='trip_id')
    df = pd.merge(left=df, right=df_routes, left_on='route_id', right_on='route_id')
    line_routes_dic = get_all_trips(df)
    save_trip_file(line_routes_dic)


