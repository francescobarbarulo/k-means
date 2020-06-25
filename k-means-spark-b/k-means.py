"""
when running in local mode the algorithm displays the means in the progress and the errors.
Also it plots the resulting clustered points graph
"""

import math
import os
import shutil

import numpy as np
from pyspark import SparkContext, Accumulator
from resources.PlotUtil import PLotUtil
import configparser as cp

sc = SparkContext(appName='k-means-app')

# global variable configurations for the program
config = cp.ConfigParser()
config.read('config.ini')
sc.setLogLevel(config.get('Spark', 'logLevel'))
input_file = config.get('Dataset', 'inputPath')
output_location = config.get('Dataset', 'outputPath')
cluster_number = config.getint('Dataset', 'numberOfClusters')
dimension = config.getint('Dataset', 'dimension')
stop_err_level = config.getfloat('K-means', 'errorThreshold')
iteration_max = config.getint('K-means', 'maximumNumberOfIterations')
iteration_min = config.getint('K-means', 'minimumNumberOfIterations')
mode = config.get('Spark', 'mode')


def closest_mean(point, means):
    distance = np.sum((np.asarray(means) - np.array(point)) ** 2, axis=1)
    return np.argmin(distance)


def shortest_distance(point, means):
    distance = np.sum((np.asarray(means) - np.array(point)) ** 2, axis=1)
    return np.amin(distance)


def main():
    if os.path.exists("./"+output_location+"/"):
        shutil.rmtree("./"+output_location+"/")

    error_distance = float('inf')

    points_in_text = sc.textFile(input_file)
    points = points_in_text.map(lambda x: x.split(",")).map(lambda x: np.array(x, dtype=float)).cache()
    starting_means = points.takeSample(num=cluster_number, withReplacement=False)
    if sc.master == 'DEBUG':
        print("starting means size ", len(starting_means))

    iteration = 0
    errors = []
    intermediate_means = sc.broadcast(starting_means)
    print("starting means ", intermediate_means.value)

    if dimension == 2 and mode == 'DEBUG':
        """ plot points and initial means with black if the dimension is 2
        for debugging purpose"""
        PLotUtil.plot_list(points.collect())
        PLotUtil.plot_list(starting_means, col='black', sz=80)

    while iteration < iteration_max:
        error_accumulator = sc.accumulator(0)
        previous_error_distance = error_distance
        accumulator = (np.zeros(shape=(dimension,), dtype=float), 0)
        new_means = points.map(lambda x: (closest_mean(x, intermediate_means.value), x)) \
            .aggregateByKey(accumulator, lambda a, b: (a[0] + b, a[1] + 1), lambda a, b: (a[0] + b[0], a[1] + b[1])) \
            .mapValues(lambda v: v[0] / v[1]).values().collect()

        ''' 
        ## commented because of a better version, found next to this block, works more efficiently.
        # it basically uses an accumulator
        error_distance = points.aggregate(0.0, lambda a, x: shortest_distance(x, intermediate_means.value) + a, \
                                          lambda a, b: (a + b))
        '''

        points.map(lambda x: shortest_distance(x, intermediate_means.value)).foreach(lambda x: error_accumulator.add(x))
        error_distance = error_accumulator.value
        intermediate_means = sc.broadcast(new_means)
        iteration += 1

        # display debugging information if it is in debug mode
        if mode == 'DEBUG':
            print("Means ", intermediate_means.value, " iteration ", iteration, " error ", error_distance)
            # collect the errors in a list to plot the error trend at the end
            errors.append(error_distance)

        if (iteration > iteration_min) and (
                math.fabs(previous_error_distance - error_distance) < (stop_err_level * previous_error_distance)):
            break

    print("Final Means")
    for mean in intermediate_means.value:
        print(mean)

    if dimension == 2 and mode == 'DEBUG':
        '''plotting the final means if the dimension is 2'''
        if len(starting_means[0]) == 2:
            PLotUtil.plot_list(intermediate_means.value, col='red', sz=80)
            PLotUtil.show()

            '''plotting the line graph of errors'''
            PLotUtil.plot(errors)

            '''plotting the scatter plot of the cluster'''
            PLotUtil.clustering_plot(points.collect(), intermediate_means.value, closest_mean)

    '''Saving the output cluster centroids'''
    sc.parallelize(intermediate_means.value).saveAsTextFile("./"+output_location+"/")
    sc.cancelAllJobs()
    sc.stop()


if __name__ == '__main__':
    main()
