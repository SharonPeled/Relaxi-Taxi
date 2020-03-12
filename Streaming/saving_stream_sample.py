import pandas as pd
from pyproj import Proj,transform
from shapely.geometry import Point
from shapely.geometry import LineString
from Data_Preprocessing.Bus_Routes.generate_routes import transform_coordinates

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


def adjust_stream_data(stream):
    # stream["timestamp"] = pd.to_datetime(stream["timestamp"]).apply(lambda elem:elem.replace(second=0))
    stream["point"] = stream.apply(
        lambda row: transform_coordinates(row["longitude"],row["latitude"]),axis=1)
    return stream


if __name__ == '__main__':
    stream = pd.read_csv("..//Data//Samples//3days3lines.csv",dtype={"lineId":str,"vehicleId":str})
    print(stream.shape)
    stream = adjust_stream_data(stream)
    stream.to_csv("..//Data//Samples//3days3lines_adjusted.csv",index=False)
    # stream["test"] = stream.apply(lambda row : row["point"].distance(stream.iloc[0]["point"]),axis=1)
    print(stream)