import numpy as np
import matplotlib.pyplot as plt
import os.path
import sys


def parse_args():
    if '-n' not in sys.argv or '-d' not in sys.argv:
        print('Usage: python random_points.py -n <number_of_points> -d <point_dimension>')
        exit(1)

    try:
        n = int(sys.argv[sys.argv.index('-n') + 1])
        d = int(sys.argv[sys.argv.index('-d') + 1])
        return n, d
    except:
        print('Usage: python random_points.py -n <number_of_points> -d <point_dimension>')
        exit(1)


def main():
    n, d = parse_args()

    # set random seed to repeat
    np.random.seed(42)

    #lists for a plot
    xs = []
    ys = []

    if os.path.exists("./points.txt"):
        os.remove("./points.txt")

    with open("points.txt", "w") as f:
        for i in range(0, n):
            pt = 10 * np.random.random(d)
            xs.append(pt[0])
            ys.append(pt[1])
            f.write(','.join('{:.3f}'.format(x) for x in pt))
            f.write('\n')

    plt.scatter(xs,ys)
    plt.show()


if __name__ == '__main__':
    main()
