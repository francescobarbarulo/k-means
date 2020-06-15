"""
This is a Plot utility class used for plotting the result if the code is running in the local mode.
"""
import matplotlib.pyplot as plt


class PLotUtil:

    @staticmethod
    def plot_list(points, col='blue', mark='o', sz=20):
        # plotting list of points
        x, y = [], []
        for pt in points:
            x.append(pt[0])
            y.append(pt[1])
        plt.scatter(x, y, color=col, marker=mark, s=sz)

    @staticmethod
    def clustering_plot(points, means, closest_mean):
        for pt in points:
            if closest_mean(pt, means) == 0:
                plt.scatter(pt[0], pt[1], color='red')
            elif closest_mean(pt, means) == 1:
                plt.scatter(pt[0], pt[1], color='blue')
            elif closest_mean(pt, means) == 2:
                plt.scatter(pt[0], pt[1], color='green')
            elif closest_mean(pt, means) == 3:
                plt.scatter(pt[0], pt[1], color='yellow')
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
