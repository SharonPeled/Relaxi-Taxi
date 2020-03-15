import pandas as pd
df = pd.read_csv("..//Data_Preprocessing//Bus_Routes//clean_trips_full.csv")
all_routes = list(sorted(set(df["trip_id"].values)))


import pickle
with open('all_routes.pkl', 'wb') as f:
    pickle.dump(all_routes, f)