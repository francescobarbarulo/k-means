import numpy as np

number_of_points = 1000
number_of_dimensions = 7

data_label = np.full(shape=(number_of_points, 1), fill_value="DATA", dtype=np.object_)
data_id = np.arange(1, number_of_points + 1, 1, dtype=np.int32).reshape((number_of_points, 1))
data_points = (np.random.rand(number_of_points, number_of_dimensions)*number_of_points).astype(np.object_)

generated_points = np.concatenate((data_id, data_points), axis=1)
generated_points = np.concatenate((data_label, generated_points), axis=1)

np.savetxt("kmeans_data.txt", generated_points, delimiter=",", fmt="%s")