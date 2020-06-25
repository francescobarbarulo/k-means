"""
This is a Plot utility class used for plotting the result if the code is running in the local mode.
"""
import matplotlib.pyplot as plt
import numpy as np


class PLotUtil:

    @staticmethod
    def plot_list(points, col='blue', mark='o', sz=20):
        # plotting list of points
        points = np.asarray(points)
        x = points[:, 0]
        y = points[:, 1]
        plt.scatter(x, y, color=col, marker=mark, s=sz)

    @staticmethod
    def clustering_plot(points, means, closest_mean):
        for point in points:
            selected_color = 'red' if closest_mean(point, means) == 0 else \
                'blue' if closest_mean(point, means) == 1 else \
                'green' if closest_mean(point, means) == 2 else \
                'yellow' if closest_mean(point, means) == 3 else None
            plt.scatter(point[0], point[1], color=selected_color)

        PLotUtil.plot_list(means, col='black', mark='*', sz=400)
        plt.show()

    @staticmethod
    def show():
        plt.show()

    '''Plotting error values'''

    @staticmethod
    def plot(values):
        plt.plot(values)
        plt.show()
