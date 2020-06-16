import sys
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.cm as cm


def closest_mean(point, centroids):
    matrix_of_point = np.tile(point, (centroids.shape[0], 1))

    squared_distance = np.sum(((matrix_of_point - centroids) ** 2), axis=1)

    # Take the minimum distance (first one in case of multiple equal distances).
    closest_mean_index = np.where(squared_distance == squared_distance.min())[0][0]

    return closest_mean_index


def main():
    centroids_f = sys.argv[1]
    dataset_f = sys.argv[2]

    centroids = np.loadtxt(centroids_f, float, delimiter=',')
    points = np.loadtxt(dataset_f, float, delimiter=',')

    colors = cm.rainbow(np.linspace(0.3, 0.9, len(centroids)))

    for p in points:
        index = closest_mean(p, centroids)
        plt.scatter(p[0], p[1], color=colors[index])

    for c, color in zip(centroids, colors):
        plt.scatter(c[0], c[1], color='black')

    plt.show()


if __name__ == '__main__':
    main()
