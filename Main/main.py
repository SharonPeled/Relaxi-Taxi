import pandas as pd
import numpy as np
from collections import defaultdict, Counter
from datetime import datetime, timedelta
import json
from threading import Lock, Thread
from Real_Time_System.RTS import RTS
from time import time

FILE_TO_STREAM = "/datashare/busFile"
PICKED_COLUMNS = ['longitude', 'latitude', 'lineId', 'timestamp','journeyPatternId', 'vehicleId']
UPDATE_FREQUENCY = 60 # seconds


def transform_ping(ping_str):
    ping_json = json.loads(ping_str)
    ping_json['timestamp'] = datetime.fromtimestamp(int(ping_json['timestamp']['$numberLong'])/1000) # ms
    return pd.DataFrame({k : ping_json[k] for k in PICKED_COLUMNS},index=[0])


if __name__ == '__main__':
    rts = RTS()
    with open("..//Data//Samples//sample50k.txt","r") as streamer:
        ping = transform_ping(next(iter(streamer)))
        last_update = ping.iloc[0]["timestamp"]
        pings_chunk = [ping]
        sys_beg = ping.iloc[0]["timestamp"]
        tot_beg = time()
        tot_rec = 0


        for i, line in enumerate(streamer):
            ping = transform_ping(line)
            pings_chunk.append(ping)
            # updating system
            if (ping.iloc[0]["timestamp"] - last_update).seconds > UPDATE_FREQUENCY:
                beg = time()
                rts.recieveDataStream(pings_chunk)
                tot_rec += time()-beg
                print()
                print("System time: ", rts.system_time_)
                print("Total system time pass: ",(ping.iloc[0]["timestamp"] - sys_beg).seconds)
                print("Receive time: ",time()-beg)
                print("Total receive time: ",tot_rec)
                print("Total time: ",time()-tot_beg)
                pings_chunk = []
                last_update = max(last_update,ping.iloc[0]["timestamp"])
                print()
                if time()-tot_beg > 120:
                    break


