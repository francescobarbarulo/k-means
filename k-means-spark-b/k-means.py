"""when running in local mode the algorithm displays the means in the progress and the errors.
Also it plots the resulting clustered points graph"""

import math
import os
import shutil
import sys

import numpy as np
from pyspark import SparkContext
from resources.PlotUtil import PLotUtil

sc = SparkContext('local', 'k-means-app')
sc.setLogLevel('WARN')


def closest_mean(point, means):
    j = 0
    shortest_dist = math.pow(np.linalg.norm(np.subtract(means[j], point)), 2)
    nearest_index = j
    j = j + 1
    while j < len(means):
        distance = math.pow(np.linalg.norm(np.subtract(means[j], np.array(point))), 2)
        if shortest_dist > distance:
            nearest_index = j
            shortest_dist = distance
        j = j + 1
    return nearest_index


def shortest_distance(point, means):
    j = 0
    shortest_dist = math.pow(np.linalg.norm(np.subtract(means[j], point)), 2)
    j = j + 1
    while j < len(means):
        distance = math.pow(np.linalg.norm(np.subtract(means[j], point)), 2)
        if shortest_dist > distance:
            shortest_dist = distance
        j += 1
    return shortest_dist


def main():
    if len(sys.argv) == 1:
        input_f = "./points.txt"
        mean_number = 4
    elif len(sys.argv) == 3:
        input_f = sys.argv[1]
        mean_number = int(sys.argv[2])
    else:
        print("usage: python </path/to/inputfile.txt> <number_of_means> \n or no arguments")
        exit(0)

    if os.path.exists("./output/"):
        shutil.rmtree("./output/")

    err_distance = float('inf')
    stop_err_level = 0.0000001
    iteration_max = 20
    iteration_min = 5

    pointstxt = sc.textFile(input_f)
    points = pointstxt.map(lambda x: x.split(",")).map(lambda x: np.array(x, dtype=float))
    starting_means = points.takeSample(num=mean_number, withReplacement=False)
    if sc.master == 'local':
        print("starting means size ", len(starting_means))

    iteration = 0
    errs = []
    interm_means = sc.broadcast(starting_means)
    print("starting means ", interm_means.value)

    if len(starting_means[0]) == 2:
        """ plot points and initial means with black if the dimension is 2
        for debugging purpose"""
        PLotUtil.plot_list(points.collect())
        PLotUtil.plot_list(starting_means, col='black', sz=80)

    while iteration < iteration_max:
        prev_errdist = err_distance
        aTuple = (0, 0)
        new_means = points.map(lambda x: (closest_mean(x, interm_means.value), x))\
            .aggregateByKey(aTuple, lambda a, b: (a[0] + b, a[1] + 1), lambda a, b: (a[0] + b[0], a[1] + b[1])) \
            .mapValues(lambda v: v[0] / v[1]).values().collect()

        err_distance = points.map(lambda x: shortest_distance(x, interm_means.value)).sum()

        interm_means = sc.broadcast(new_means)
        iteration += 1

        # display debugging information if it is in local node
        if sc.master == 'local':
            print("Means ", interm_means.value, " iteration ", iteration, " error ", err_distance)
            # collect the errors in a list to plot the error trend at the end
            errs.append(err_distance)

        if (iteration > iteration_min) and (math.fabs(prev_errdist - err_distance) < (stop_err_level * prev_errdist)):
            break

    if sc.master == 'local':
        print("Final Means")
        for mean in interm_means.value:
            print(mean)

        '''plotting the final means if the dimension is 2'''
        if len(starting_means[0]) == 2:
            PLotUtil.plot_list(interm_means.value, col='red', sz=80)
            PLotUtil.show()

            '''plotting the line graph of errors'''
            PLotUtil.plot(errs)

            '''plotting the scatter plot of the cluster'''
            PLotUtil.clustering_plot(points.collect(), interm_means.value, closest_mean)

    '''Saving the output clusters'''
    sc.parallelize(interm_means.value).saveAsTextFile("./output/")
    sc.cancelAllJobs()
    sc.stop()


if __name__ == '__main__':
    main()
