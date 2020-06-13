import numpy as np

number_of_points = 1000
number_of_dimensions = 7

data_points = (np.random.rand(number_of_points, number_of_dimensions)*number_of_points).astype(np.object_)
np.savetxt("kmeans_data.txt", data_points, delimiter=",", fmt="%s")