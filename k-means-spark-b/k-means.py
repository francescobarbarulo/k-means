import math
import os
import shutil
import sys

import numpy as np
from pyspark import SparkContext
from resources.PlotUtil import PLotUtil

sc = SparkContext('local', 'k-means-app')
sc.setLogLevel('WARN')


def closest_mean(point, means, repetition):
    j = 0
    shortest_dist: float = np.linalg.norm(np.subtract(means[j], point))
    nearest_index = j
    j = j + 1
    while j < repetition:
        distance = np.linalg.norm(np.subtract(means[j], np.array(point)))
        if shortest_dist > distance:
            nearest_index = j
            shortest_dist = distance
        j = j + 1
    return nearest_index


def shortest_distance(point, means, repetition):
    j = 0
    shortest_dist = np.linalg.norm(np.subtract(means[j], point))
    j = j + 1
    while j < repetition:
        distance = np.linalg.norm(np.subtract(means[j], point))
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
    stop_err_level = 0.01
    iteration_max = 40

    pointstxt = sc.textFile(input_f)
    points = pointstxt.map(lambda x: x.split(",")).map(lambda x: np.array(x, dtype=float))
    starting_means = points.takeSample(num=mean_number, withReplacement=False)
    print("starting means size ", len(starting_means))

    iteration = 0
    errs = []
    interm_means = sc.broadcast(starting_means)
    mean_count = sc.broadcast(mean_number)
    print("starting means ", interm_means.value)

    if len(starting_means[0]) == 2:
        # plot points and initial means with black if the dimension is 2
        PLotUtil.plot_list(points.collect())
        PLotUtil.plot_list(starting_means, col='black', sz=80)

    while iteration < iteration_max:
        prev_errdist = err_distance
        new_means = points.map(lambda x: (closest_mean(x, interm_means.value, mean_count.value), x))\
            .reduceByKey(lambda x, y: np.average(np.array([x, y]), axis=0)).values().collect()

        iteration += 1
        if iteration > 0:
            interm_means = sc.broadcast(new_means)

        err_distance = points.map(lambda x: shortest_distance(x, interm_means.value, mean_count.value)).sum()
        errs.append(err_distance)

        print(" means num ", len(interm_means.value), " iteration ", iteration, " error ", err_distance)

        if (iteration > 5) and (math.fabs(prev_errdist - err_distance) < (stop_err_level * prev_errdist)):
            break

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
