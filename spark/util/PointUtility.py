import numpy as np


class PointUtility:

    @staticmethod
    def parse_point(row):
        return np.array(row.split(','), dtype=np.float64)

    @staticmethod
    def get_closest_mean(point, means):
        means_matrix = np.array(means)
        matrix_of_point = np.tile(point, (means_matrix.shape[0], 1))

        squared_distance = np.sum(((matrix_of_point - means_matrix) ** 2), axis=1)

        # Take the minimum distance (first one in case of multiple equal distances).
        closest_mean_index = np.where(squared_distance == squared_distance.min())[0][0]

        # Tuple used to make the ndarray hashable in reduceByKey()
        return tuple(means[closest_mean_index]), (point, 1)

    @staticmethod
    def sum_partial_means(partial_c1, partial_c2):
        return partial_c1[0] + partial_c2[0], partial_c1[1] + partial_c2[1]

    @staticmethod
    def compute_new_mean(partial_mean):
        # partial_mean is a tuple (old_mean, (sum_of_points, number_of_points))
        partial_sum = partial_mean[1]
        new_mean = partial_sum[0]/partial_sum[1]

        return new_mean

    @staticmethod
    def compute_min_squared_distance(point, means):
        means_matrix = np.array(means)
        matrix_of_point = np.tile(point, (means_matrix.shape[0], 1))
        squared_distance = np.sum(((matrix_of_point - means_matrix) ** 2), axis=1)

        return squared_distance.min()

    @staticmethod
    def to_string(point):
        return np.array2string(point, separator=',')[1:-1].replace(' ', '')