import numpy as np
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
    except ValueError:
        print('Usage: python random_points.py -n <number_of_points> -d <point_dimension>')
        exit(1)


def main():
    n, d = parse_args()

    # set random seed to repeat
    np.random.seed(42)

    if os.path.exists("./points.txt"):
        os.remove("./points.txt")

    with open("points.txt", "w") as f:
        for i in range(0, n):
            pt = 10 * np.random.random(d)
            f.write(','.join('{:.3f}'.format(x) for x in pt))
            f.write('\n')


if __name__ == '__main__':
    main()
