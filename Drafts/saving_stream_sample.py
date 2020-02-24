import pandas as pd
from pyproj import Proj,transform
from shapely.geometry import Point
from shapely.geometry import LineString
from Data_Preprocessing.Bus_Routes.generate_routes import transform_coordinates

def adjust_stream_data(stream):
    stream["timestamp"] = pd.to_datetime(stream["timestamp"]).apply(lambda elem:elem.replace(second=0))
    stream["point"] = stream.apply(
        lambda row: transform_coordinates(row["longitude"],row["latitude"]),axis=1)
    return stream


stream = pd.read_csv("..//Data//Samples//3days3lines.csv",dtype={"lineId":str,"vehicleId":str})
print(stream.shape)
stream = stream[:500]
stream = adjust_stream_data(stream)
stream.to_csv("..//Data//Samples//3days3lines_adjusted_small.csv",index=False)
print(stream)