import sys
import numpy as np
import random
import math

class Point:
    def __init__(self, x, y):
        self.x = x
        self.y = y

    def __str__(self):
        return str((self.x, self.y))


class Circle:
    def __init__(self, origin, radius):
        self.origin = origin
        self.radius = radius


def clusterable_points(number_of_points):
    centers = [[25, 75], [50, 25], [75, 75]]
    origin = Point(0, 0)
    radius = 20

    values = np.empty(shape=(0, 2), dtype=float)
    circle = Circle(origin, radius)
    for center in centers:
        center_x, center_y = center
        for i in range(0, number_of_points):
            p = random.random() * 2 * math.pi
            r = circle.radius * math.sqrt(random.random())
            x, y = math.cos(p) * r + center_x, math.sin(p) * r + center_y
            values = np.concatenate((values, np.array([[x, y]], dtype=float)), axis=0)

    filename = "{}-2d-clusterbale-points.txt".format(number_of_points * len(centers))
    np.savetxt(filename, values, delimiter=",", fmt="%s")
    print("Generated {}".format(filename))


def random_points(number_of_points, number_of_dimensions):
    data_points = (np.random.rand(number_of_points, number_of_dimensions) * number_of_points).astype(np.object_)
    filename = "{}-{}d-points.txt".format(number_of_points, number_of_dimensions)
    np.savetxt(filename, data_points, delimiter=",", fmt="%s")
    print("Generated {}".format(filename))


def usage():
    return "Usage: python main.py <number_of_points> <number_of_dimensions> [<type>]\n" \
           "- type = {random, cluster}"


def main(_, number_of_points=None, number_of_dimensions=None, t='random'):
    if number_of_points is None or number_of_dimensions is None:
        print(usage())
        return

    try:
        if t == 'cluster':
            clusterable_points(int(number_of_points))
        else:
            random_points(int(number_of_points), int(number_of_dimensions))
    except ValueError:
        print(usage())


if __name__ == '__main__':
    main(*sys.argv)
