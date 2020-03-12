import pandas as pd
import numpy as np
from collections import defaultdict
from pyproj import Proj,transform, Transformer
from shapely.geometry import Point
NUM_DIFFERENT_STOPS_ALLOWED = 3
TRANSFORMER = Transformer.from_crs('EPSG:4326', 'EPSG:2157', always_xy=True)


def devide_to_trips(df):
    trips_object = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: dict())))  # route - trip - records
    for attrs, trip_df in df.groupby(["route_id", "route_short_name", "direction_id", "trip_id"]):
        (route_id, line, direction, trip_id) = attrs
        trips_object[route_id][line][direction][trip_id] = trip_df.sort_values(["stop_sequence"]).reset_index()
    return trips_object


def is_same_route(trip_df1,trip_df2):
    # Checking if the two trip has the same route
    # if there's a different stop with the same stop_sequence than the trips are different
    # to avoid error of jump between stops (from 56 to 58)
    stop_sequence_dic = defaultdict(lambda : list())
    for (i,row1), (j,row2) in zip(trip_df1.iterrows(),trip_df2.iterrows()):
        stop_sequence_dic[row1["stop_sequence"]].append(row1["stop_id"])
        stop_sequence_dic[row2["stop_sequence"]].append(row2["stop_id"])
    num_differ_stops = 0    # number of stops the 2 routes are differ
    for stop_sequence, stop_ids_list in stop_sequence_dic.items():
        if len(stop_ids_list) == 1: # only one trip pass through this stop
            num_differ_stops += 1
            continue
        if stop_ids_list[0] != stop_ids_list[1]:
            return False
    return num_differ_stops <= NUM_DIFFERENT_STOPS_ALLOWED


def calculate_route_trip_errors(trips_object):
    error_counter = defaultdict(lambda: 0)
    for route, line_dic in trips_object.items():
        for line, direction_dic in line_dic.items():
            for direction, trip_dic in direction_dic.items():
                random_trip = next(iter(trip_dic.values()))
                error_counter[(route, line, direction)] = sum(
                    [not is_same_route(random_trip, trip) for trip in trip_dic.values()])
    return error_counter


def get_clean_routes(trips_object,error_counter):
    # Each route_id has a single line and 1 or 2 directions!
    routes_df_list = []
    for attrs, error in error_counter.items():
        print(attrs,error)
        (route, line, direction) = attrs
        if error != 0:
            continue
        routes_df_list.append(get_complete_route(trips_object[route][line][direction].values()))
    return pd.concat(routes_df_list)

def get_complete_route(trips): # TODO: check that the dist_traveled is increasing as we go further in stop_sequence
    route_df = pd.DataFrame(columns=next(iter(trips)).columns)
    for trip in trips:
        route_df = route_df.append(trip[~trip["stop_sequence"].isin(route_df["stop_sequence"])])
    return clean_route(route_df)

def clean_route(route_df):
    route_df = route_df.sort_values(["stop_sequence"]).reset_index()
    route_df["stop_sequence"] = range(1,route_df.shape[0]+1)
    route_df["dist_traveled"] = route_df["shape_dist_traveled"]
    route_df = route_df[
        ['route_id', 'route_short_name', 'direction_id', 'stop_sequence', "stop_id", 'stop_lat', 'stop_lon',
         'dist_traveled']]
    route_df["stop_point"] = route_df.apply(
        lambda row: transform_coordinates(row["stop_lon"],row["stop_lat"]),axis=1)
    return route_df

def transform_coordinates(longitude,latitude):
    # transforms long, latt to Irish coordinate points.
    # trans = transform(Proj(init='EPSG:4326'), Proj(init='EPSG:2157'), longitude, latitude)  # longitude, latitude
    trans = TRANSFORMER.transform(longitude, latitude)
    return Point(trans[0],trans[1])


def main():
    df_stops = pd.read_csv("..//..//Data//Google_transit//stops.txt")
    df_stop_times = pd.read_csv("..//..//Data//Google_transit//stop_times.txt")
    df_trips = pd.read_csv("..//..//Data//Google_transit//trips.txt")
    df_routes = pd.read_csv("..//..//Data//Google_transit//routes.txt")
    df = pd.merge(left=df_stops, right=df_stop_times, left_on='stop_id', right_on='stop_id')
    df = pd.merge(left=df, right=df_trips, left_on='trip_id', right_on='trip_id')
    df = pd.merge(left=df, right=df_routes, left_on='route_id', right_on='route_id')
    trips_object = devide_to_trips(df)
    error_counter = calculate_route_trip_errors(trips_object)
    routes = get_clean_routes(trips_object, error_counter)
    routes.to_csv("clean_routes.csv",index=False)


if __name__ == '__main__':
    main()

