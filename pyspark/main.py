#import numpy as np
import sys
import pprint
from operator import add
from pyspark import SparkContext

master = "local[*]"
pp = pprint.PrettyPrinter(indent=2).pprint

def cast_list(x):
    Y = []
    for i in x:
        Y.append(float(i))
    return Y

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print('Usage: ' + sys.argv[0] + ' <input file> <n>', file=sys.stderr)
        sys.exit(1)

    INPUT_FILE = sys.argv[1]
    MEANS_N = int(sys.argv[2])

    sc = SparkContext(master, "KMeans")
    
    pointstxt = sc.textFile("./" + INPUT_FILE)
    points_list = pointstxt.map(lambda x: x.split(","))
    points = points_list.map(lambda x: cast_list(x))
    means = points.takeSample(num = MEANS_N, withReplacement=False)
    print(means)

    
