import pandas as pd
import numpy as np
from random import randrange
from sklearn.linear_model import LinearRegression
import pickle


def generate_features(X_data,y):
    time_passed = (pd.to_datetime(X_data.tail(1)['timestamp'].values[0]) - pd.to_datetime(X_data.head(1)['timestamp'].values[0])).seconds

    distance_traveled = X_data.tail(1)['dist_traveled'].values[0]
    avg_distances = np.average(X_data['dist_to_line'])
    remaining_distance = y['dist_traveled'] - X_data.tail(1)['dist_traveled'].values[0]

    return [time_passed,distance_traveled,avg_distances,remaining_distance]




def build_training_data(df):
    index = {}
    routes = drives['route_id'].unique()
    for i, route in enumerate(routes):
        index[route] = i

    number_of_routes = len(routes)
    number_of_samples = drives.tail(1)['sample_number'].values[0]
    number_of_features = 4
    number_of_data_points = 5

    dataset = np.zeros((number_of_samples * number_of_data_points, number_of_routes * 4))
    target = np.zeros((number_of_samples * number_of_data_points,))

    for i, sample in drives.groupby('sample_number'):
        current_route = sample.head(1)['route_id'].values[0]
        num_of_pings = len(sample)
        for j in range(number_of_data_points):
            X_slice = randrange(1, num_of_pings - 3, 1)
            y_index = randrange(X_slice + 1, num_of_pings)
            X_data = sample.iloc[0:X_slice]
            y = sample.iloc[y_index]

            dataset[j + number_of_data_points * (i - 1)][4 * (index[current_route]):4 * (index[current_route] + 1)] = generate_features(X_data,y)

            target[j + number_of_data_points * (i - 1)] = (pd.to_datetime(y['timestamp']) - pd.to_datetime(X_data.tail(1)['timestamp'].values[0])).seconds

    return dataset,target




drives = pd.read_csv('full_drive_sample.csv')
X,y = build_training_data(drives)
X_test,y_test = build_training_data(drives)
reg = LinearRegression().fit(X, y)
pickle.dump(reg,open("DelayModel", 'wb'))

loaded_model = pickle.load(open("DelayModel",'rb'))
print("R^2 = ",reg.score(X_test, y_test))

