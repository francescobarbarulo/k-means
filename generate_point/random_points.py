import scipy.sparse as sparse
import scipy.stats as stats
import numpy as np
import os.path

# Number of points to be generated
N = 200;

# Dimention of the points
d = 10;

# set random seed to repeat
np.random.seed(42)

# create a vector density 0.25
if os.path.exists("./points.txt"):
    os.remove("./points.txt")

with open("points.txt","w") as f:
    for i in range(0,N):
        pt = 10 * np.random.random(d)
        for j in range(0,d):
            f.write("{:.3f}".format(pt[j]))
            f.write(",")
        f.write("\n")
        #f.write("\n",)