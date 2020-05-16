import sys
import numpy as np
import random
import pprint
from operator import add
from pyspark import SparkContext

master = "local[*]"
pp = pprint.PrettyPrinter(indent=2).pprint

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print('Usage: ' + sys.argv[0] + ' <input file> <n>', file=sys.stderr)
        sys.exit(1)

    CONF_INPUT_FILE = sys.argv[1]
    CONF_N = int(sys.argv[2])

    sc = SparkContext(master, "KMeans")

    textfile = sc.textFile(CONF_INPUT_FILE)
    m = textfile.map(lambda x: ( ( int(random.uniform(0, CONF_N)), np.fromstring(x, dtype=float, sep=',') )))

    n = m.reduceByKey(add)  # TODO: how?

    pp(n.collect())
    pp(n.count())

