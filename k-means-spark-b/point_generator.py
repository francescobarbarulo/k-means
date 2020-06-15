"""
used for generating clusterable points in a circular manner.
"""

import numpy as np
import random
import math
from resources.PlotUtil import PLotUtil


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


if __name__ == '__main__':
    num_samples = 100
    centers = [[25, 75], [50, 25], [75, 75]]
    origin = Point(0, 0)
    radius = 20

    values = np.empty(shape=(0, 2), dtype=float)
    circle = Circle(origin, radius)
    for center in centers:
        center_x, center_y = center
        for i in range(0, 100):
            p = random.random() * 2 * math.pi
            r = circle.radius * math.sqrt(random.random())
            x, y = math.cos(p) * r + center_x, math.sin(p) * r + center_y
            values = np.concatenate((values, np.array([[x, y]], dtype=float)), axis=0)

    PLotUtil.plot_list(values)
    PLotUtil.show()
    np.savetxt("pts_in_circles.txt", values, delimiter=",", fmt="%s")

