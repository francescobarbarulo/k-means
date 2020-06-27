import numpy as np


def parse_point(row):
    return np.array(row.split(','), dtype=np.float64)


def get_closest_mean(point, means):
    squared_distance = np.sum(((np.array(means) - point) ** 2), axis=1)

    # Take the minimum distance (first one in case of multiple equal distances).
    closest_mean_index = np.where(squared_distance == squared_distance.min())[0][0]

    # Tuple used to make the ndarray hashable in reduceByKey()
    return tuple(means[closest_mean_index]), (point, 1)


def sum_point_accumulators(accumulator_1, accumulator_2):
    # Each accumulator tuple has the format (sum_of_points, number_of_points)
    return accumulator_1[0] + accumulator_2[0], accumulator_1[1] + accumulator_2[1]


def compute_new_mean(accumulator):
    # accumulator is a tuple ((old_mean), (sum_of_points, number_of_points))
    new_mean = accumulator[1][0]/accumulator[1][1]

    return new_mean


def compute_min_squared_distance(point, means):
    squared_distance = np.sum(((np.array(means) - point) ** 2), axis=1)
    return squared_distance.min()


def to_string(point):
    return np.array2string(point, separator=',')[1:-1].replace(' ', '')