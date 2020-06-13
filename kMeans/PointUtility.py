import numpy as np


class PointUtility:

    @staticmethod
    def parse_point(row):
        return np.array(row.split(','), dtype=np.float64)

    @staticmethod
    def cast_to_dictionary(point_mean_tuple):
        # Tuple used to make the ndarray hashable in reduceByKey(). Used as key.
        point = tuple(point_mean_tuple[0])

        # Dictionary used as value. Necessary to replicate the point to compute the distance in isolation.
        dictionary = {"Point": point_mean_tuple[0], "Mean": point_mean_tuple[1]}

        return point, dictionary

    @staticmethod
    def get_closest_mean(dictionary1, dictionary2):
        squared_distance1 = np.sum((dictionary1["Point"] - dictionary1["Mean"])**2)
        squared_distance2 = np.sum((dictionary2["Point"] - dictionary2["Mean"])**2)

        return dictionary1 if squared_distance1 <= squared_distance2 else dictionary2

    @staticmethod
    def cast_to_tuple(point_dict_tuple):
        # Tuple used to make the ndarray hashable in reduceByKey()
        closest_mean = tuple(point_dict_tuple[1]["Mean"])

        # The first element is a tuple of a single element, because of the previous use as a key.
        # Now it is considered as a value, so the tuple must be reconverted to an ndarray.
        point = np.array(point_dict_tuple[0])

        return closest_mean, (np.array(point), 1)

    @staticmethod
    def sum_partial_means(partial_c1, partial_c2):
        return partial_c1[0] + partial_c2[0], partial_c1[1] + partial_c2[1]

    @staticmethod
    def compute_new_mean(old_new_mean_tuple):
        old_mean = old_new_mean_tuple[0]
        new_mean = old_new_mean_tuple[1][0]/old_new_mean_tuple[1][1]
        distance = (np.sum((old_mean - new_mean)**2))**(1/2)

        return old_mean, new_mean, distance

    @staticmethod
    def to_string(point):
        return np.array2string(point, separator=',')[1:-1].replace(' ', '')