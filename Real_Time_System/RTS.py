import pandas as pd
import numpy as np
from collections import defaultdict, Counter
from shapely.geometry import Point
from shapely.geometry import LineString
from shapely import wkt
from datetime import datetime, timedelta

pd.set_option('display.max_columns', 500)

UNASSIGNED = -1
MIN_NUM_OF_PINGS = 5 # before that we won't try to assign a drive or remove a drive
MIN_VALID_PINGS = 5 # minimum number of valid pings needed to assign a drive
MAX_PINGS_ERROR = 75 # max average distance from the line allowed for the valid pings (in meters)
MAX_SINGLE_PING_ERROR = 150 # max distance to line a ping can have for it to be considered as valid.
MAX_TIME_BETWEEN_PINGS = 600 # if a drive didn't got a ping for that long it will be removed (seconds)
MAX_UNASSIGNED_PINGS = 50 # max number of pings a drive could have while being unassigned, otherwise the drive will be removed.
MAX_DISTANCE_TO_END_STOP = 50 # meters


class Route:
    # Represents a general route of a bus line
    def __init__(self, route_df):
        self.direction_ = route_df.iloc[0]["direction_id"]
        self.routeId_ = route_df.iloc[0]["trip_id"]
        self.line_ = route_df.iloc[0]["line_id"]
        self.route_ = route_df[["stop_sequence", "stop_id", "stop_point", "dist_traveled"]]
        self.linestring_ = LineString(list(self.route_["stop_point"]))
        self.stops_dist_from_beg_ = [self.linestring_.project(Point(coords)) for coords in self.linestring_.coords]

    def getProjectedPoints(self, points):
        proj_points = defaultdict(lambda: list())
        for point in points:
            proj_points["dist_to_line"].append(self.linestring_.distance(point))
            proj_points["dist_traveled"].append(self.linestring_.project(point))
            proj_points["proj_point"].append(self.linestring_.interpolate(self.linestring_.project(point)))
        return proj_points

    # Score - the lower the better
    def getDriveRouteScore(self, points, last_valid_ping):
        proj_points = self.getProjectedPoints(points)
        valid_signal = self.getValidPings(points, last_valid_ping["point"])
        if sum(valid_signal) < MIN_VALID_PINGS:
            return np.inf, None
        score = np.array(proj_points["dist_to_line"]).mean()
        return score, valid_signal

    def getValidPings(self, points, last_valid_point):
        proj_points = self.getProjectedPoints(points)
        last_valid_ping_proj_dist_traveled = self.getProjectedPoints([last_valid_point])["dist_traveled"][0]
        valid_signal = []
        for i in range(len(points)):
            if proj_points["dist_traveled"][i] >= last_valid_ping_proj_dist_traveled \
                    and proj_points["dist_to_line"][i] < MAX_SINGLE_PING_ERROR:
                valid_signal.append(1)
                last_valid_ping_proj_dist_traveled = proj_points["dist_traveled"][i]
            else:
                valid_signal.append(0)
        return valid_signal

    def getNextStop(self, point):
        proj_point = self.getProjectedPoints([point])
        return np.searchsorted(self.stops_dist_from_beg_, proj_point["dist_traveled"][0], side='right') + 1

    def isFinalStop(self, point):
        next_stop_num = self.getNextStop(point)
        # redundant check
        if next_stop_num < 1 or next_stop_num > len(self.linestring_.coords) + 1:
            print(next_stop_num, len(self.linestring_.coords))
            raise Exception("Stop number doesn't exists: {}.".format(next_stop_num))
        return next_stop_num == len(self.linestring_.coords) or next_stop_num == len(
            self.linestring_.coords) + 1 or point.distance(
            Point(self.linestring_.coords[-1])) < MAX_DISTANCE_TO_END_STOP


class Routes:
    def __init__(self, routes_filename):
        df_trips = pd.read_csv(routes_filename)
        df_trips["stop_point"] = df_trips["stop_point"].apply(lambda elem: wkt.loads(elem))
        self.trips_ = defaultdict(lambda: [[], []])  # key: line, value: list of 2 lists of dfs (one for each direction)
        for attrs, trip_df in df_trips.groupby(["line_id", "direction_id", "trip_id"]): # trip_id is the full identifier !!
            (line_id, direction_id, trip_id) = attrs
            self.trips_[line_id][direction_id].append(Route(trip_df))
        print(list(self.trips_.keys()))

    def __getitem__(self, line, direction = -1):
        if direction not in [0,1]:
            return self.trips_[line][0] + self.trips_[line][1]
        else:
            return self.trips_[line][direction]


class BusDrive:

    save_drives = False
    drive_index = 1
    df_full_drives = pd.DataFrame(
        columns=["sample_number", "route_id", "ping_number", "timestamp", "dist_traveled", "dist_to_line"])

    def __init__(self, routes, pings_df):
        self.routes_ = routes
        self.line_ = pings_df.iloc[0]["lineId"]
        self.vehicle_ = pings_df.iloc[0]["vehicleId"]
        self.pings_df_ = pd.DataFrame(columns=pings_df.columns)
        self.route_ = UNASSIGNED
        self.route_score_ = UNASSIGNED
        self.last_ping_ = None
        self.last_valid_ping_ = None
        self.addPings(pings_df)

    def __len__(self):
        return self.pings_df_.shape[0]

    def lastActivity(self):
        return self.last_ping_["timestamp"]

    def addPings(self, pings_df):
        if not pings_df[(pings_df["lineId"] != self.line_) | (pings_df["vehicleId"] != self.vehicle_)].empty:
            raise Exception("""Bus drive {}, {} got wrong pings.
                            {}""".format(self.line_, self.vehicle_, pings_df[
                (pings_df["lineId"] != self.line_) | (pings_df["vehicleId"] != self.vehicle_)]))
        # sorting the pings
        pings_df.sort_values(by=["timestamp"], ascending=True, inplace=True)
        pings_df.reset_index(inplace=True)
        # just for the first time
        if self.last_valid_ping_ is None or self.last_ping_ is None:
            self.last_valid_ping_ = pings_df.iloc[0]  # first ping
            self.last_ping_ = pings_df.iloc[pings_df.shape[0] - 1]  # last ping
        # filter all the pings who happened before the last ping
        pings_df = pings_df[
            pings_df["timestamp"] >= self.last_ping_["timestamp"]]
        self.last_ping_ = pings_df.iloc[pings_df.shape[0] - 1]  # last ping
        # identifying the route or adding pings to a route if it was already identify
        if self.route_ == UNASSIGNED:
            self.pings_df_ = self.pings_df_.append(pings_df,ignore_index=True)
            self.identifyPath()
        else:
            pings_df["valid_ping"] = self.route_.getValidPings(pings_df["point"].values, self.last_valid_ping_["point"])
            self.last_valid_ping_ = self.getLastValidPing(pings_df)
            self.pings_df_ = self.pings_df_.append(pings_df,ignore_index=True)

    def getLastValidPing(self, pings_df):
        temp = pings_df[pings_df["valid_ping"] == 1]
        if temp.empty:
            return self.last_valid_ping_
        return temp.iloc[-1]

    def identifyPath(self):
        if self.pings_df_.shape[0] < MIN_NUM_OF_PINGS:
            return
        #         print("identifyPath: ",self.line_)
        best_route_score = np.inf
        best_route = None
        valid_pings = None
        for route in self.routes_[self.line_]:
            score, route_valid_pings = route.getDriveRouteScore(self.pings_df_["point"].values, self.last_valid_ping_)
            if score < best_route_score:
                best_route_score = score
                best_route = route
                valid_pings = route_valid_pings

        if best_route_score <= MAX_PINGS_ERROR:
            self.route_ = best_route
            self.route_score_ = best_route_score
            self.pings_df_["valid_ping"] = valid_pings
            self.last_valid_ping_ = self.getLastValidPing(self.pings_df_)

    def isDriveEnded(self, curr_time):
        if curr_time - self.lastActivity() > timedelta(
                seconds=MAX_TIME_BETWEEN_PINGS):  # Didn't received a ping for a long time
            print("Drive Ended - No Activity: ", (self.line_, self.vehicle_))
            return True
        if self.route_ == UNASSIGNED and self.pings_df_.shape[0] > MAX_UNASSIGNED_PINGS:
            print("Drive Ended - UNASSIGNED: ", (self.line_, self.vehicle_))
            return True
        if self.route_ == UNASSIGNED:
            return False
        if self.route_.isFinalStop(self.last_ping_["point"]):
            valid_pings_df = self.pings_df_[self.pings_df_["valid_ping"] == 1]
            print("Drive Ended - Final Stop: ", (self.line_, self.vehicle_), " ", len(valid_pings_df))
            BusDrive.add_drive(valid_pings_df, self.route_.routeId_,
                               self.route_.getProjectedPoints(valid_pings_df["point"]))

            return True
        return False

    @staticmethod
    def add_drive(drive_valid_pings, route_id, proj_points):
        if not BusDrive.save_drives:
            return
        if len(drive_valid_pings) < 100:
            return
        drive_valid_pings["sample_number"] = BusDrive.drive_index
        drive_valid_pings["route_id"] = route_id
        drive_valid_pings['ping_number'] = np.arange(len(drive_valid_pings))
        drive_valid_pings["dist_traveled"] = proj_points["dist_traveled"]
        drive_valid_pings["dist_to_line"] = proj_points["dist_to_line"]
        BusDrive.df_full_drives = BusDrive.df_full_drives.append(drive_valid_pings[
                                                                     ["sample_number", "route_id", "ping_number",
                                                                      "timestamp", "dist_traveled", "dist_to_line"]],ignore_index=True)
        BusDrive.drive_index += 1

    def getRouteScore(self):
        return self.route_score_

    def isAssignedRoute(self):
        return self.route_ != UNASSIGNED

    def numValidPings(self):
        if self.route_ == UNASSIGNED:
            return None
        return self.pings_df_[self.pings_df_["valid_ping"] == 1].shape[0]


class RTS:
    def __init__(self, routes_filename):
        # Loading routes to identify and monitor
        self.routes_ = Routes(routes_filename)
        self.active_drives_ = {}
        self.system_time_ = datetime(2017, 1, 1, 0, 0)  # min system time

    def recieveDataStream(self, pings_df):
        self.system_time_ = max(self.system_time_, max(pings_df["timestamp"]))
        for attrs, drive_pings_df in pings_df.groupby(["lineId", "vehicleId"]):
            (line_id, vehicle_id) = attrs
            if (line_id, vehicle_id) in self.active_drives_.keys():
                self.active_drives_[(line_id, vehicle_id)].addPings(drive_pings_df)
            else:
                self.active_drives_[(line_id, vehicle_id)] = BusDrive(self.routes_, drive_pings_df)
        keys_to_remove = []
        for drive_key, drive in self.active_drives_.items():
            if drive.isDriveEnded(self.system_time_):
                keys_to_remove.append(drive_key)
        self.removeDrives(keys_to_remove)
        self.validateVehicleSingularity()
        self.printSystemState()

    def validateVehicleSingularity(self):
        vehicle_dic = defaultdict(lambda: [])
        for (line_id, vehicle_id) in self.active_drives_.keys():
            vehicle_dic[vehicle_id].append(line_id)
        for vehicle_id, line_list in vehicle_dic.items():
            if len(line_list) > 1:
                self.removeDuplicateVehiclesDrives(vehicle_id, line_list)
                print("New Drive Started: ", (vehicle_id, line_list))

    def removeDuplicateVehiclesDrives(self, vehicle_id, line_list):
        keys_to_remove = []
        last_activity_time = -np.inf
        last_activity_key_drive = None
        for line_id in line_list:
            drive = self.active_drives_[(vehicle_id, line_id)]
            if len(drive) < MIN_NUM_OF_PINGS:  # allow for small drives to stay for now
                continue
            # keep only the most updated drive
            if last_activity_time < drive.lastActivity():
                keys_to_remove.append(last_activity_key_drive) if last_activity_key_drive != None else None
                last_activity_time = drive.lastActivity()
                last_activity_key_drive = (vehicle_id, line_id)
        self.removeDrives(keys_to_remove)

    def removeDrives(self, drive_keys):
        for key in drive_keys:
            del self.active_drives_[key]
            print("Deleted drive: ", key)

    def printSystemState(self):
        stats_dic = defaultdict(lambda: [])
        for attrs in sorted(self.active_drives_.keys()):
            drive = self.active_drives_[attrs]
            stats_dic["drive"].append(attrs)
            stats_dic["num_pings"].append(len(drive))
            stats_dic["num_valid_pings"].append(drive.numValidPings())
            stats_dic["route_score"].append(drive.getRouteScore())
            stats_dic["found_route"].append(drive.isAssignedRoute())


if __name__ == '__main__':
    # getting the stream data
    stream = pd.read_csv("..//Data//Samples//3days3lines_adjusted.csv",dtype={"lineId":str,"vehicleId":str})
    stream["timestamp"] = pd.to_datetime(stream["timestamp"])
    stream["round_timestamp"] = stream["timestamp"].apply(lambda elem:elem.replace(second=0))
    stream["point"] = stream["point"].apply(lambda elem : wkt.loads(elem))
    # starting the RTS
    rts = RTS("..//Data_Preprocessing//Bus_Routes//clean_trips_full.csv")
    df_lists = list(stream.groupby(["round_timestamp"]))
    df_lists.sort(key=lambda elem:elem[0])
    num_hours = 5
    BusDrive.save_drives = True
    for i, (time,group_df) in enumerate(df_lists):
        print("index: {} , System time: {}".format(i,rts.system_time_))
        rts.recieveDataStream(group_df)
        if i == 60*num_hours:
            break
    BusDrive.df_full_drives.reset_index(drop=True).to_csv("drives_sample_V5.csv",index=False)
    print("FINISH")

