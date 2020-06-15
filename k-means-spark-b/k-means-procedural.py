"""
This implementation is done to be used as a benchmark to check
if the results obtained via map reduce are reasonable.
Spark context is used here to load the data and take samples only
the rest of the implementation is pure python3.6 and numpy library
"""

import numpy as np
import matplotlib.pyplot as plt
from matplotlib import style
from pyspark import SparkContext

style.use("ggplot")

sc = SparkContext('local', 'k-means-app')
sc.setLogLevel('WARN')

input_f = "./pts_in_circles.txt"
mean_number = 3
err_distance = float('inf')
stop_err_level = 0.0000001
iteration_max = 110
iteration_min = 100

points_txt = sc.textFile(input_f)
points = points_txt.map(lambda x: x.split(",")).map(lambda x: np.array(x, dtype=float))
starting_means = points.takeSample(num=mean_number, withReplacement=False)
X = points.collect()
sc.stop()
x, y = [], []
for pt in starting_means:
    x.append(pt[0])
    y.append(pt[1])
plt.scatter(x, y, c='blue', s=100)

pts = [[], [], []]
error = 0
prev_error = float('inf')
stepCnt = 0
min_iter = 1
while prev_error > error:
    if stepCnt > min_iter:
        prev_error = error
        error = 0
    stepCnt += 1
    print("step count ", stepCnt)
    for pt in X:
        shortest_dist = float('inf')
        index = -1
        for i in range(0, len(starting_means)):
            if shortest_dist > np.linalg.norm(np.subtract(starting_means[i], pt)):
                shortest_dist = np.linalg.norm(np.subtract(starting_means[i], pt))
                index = i
        if index == 0:
            pts[0].append(pt)
        elif index == 1:
            pts[1].append(pt)
        elif index == 2:
            pts[2].append(pt)

    for i in range(0, len(starting_means)):
        tot = np.array([0, 0], dtype=float)
        for j in range(0, len(pts[i])):
            tot = np.add(tot, np.array(pts[i][j]))
            error += np.linalg.norm(np.subtract(starting_means[i], pts[i][j]))
        starting_means[i] = tot / len(pts[i])

    print("error value on step ", stepCnt, " is ", error)

x, y = [], []
for pt in X:
    x.append(pt[0])
    y.append(pt[1])

plt.scatter(x, y, s=50)

x, y = [], []
for pt in starting_means:
    x.append(pt[0])
    y.append(pt[1])
plt.scatter(x, y, c='black', s=200, alpha=0.5)
plt.show()
