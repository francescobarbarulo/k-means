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


def main(_, centroids_f=None, dataset_f=None):
    if centroids_f is None or dataset_f is None:
        print("Usage: python main.py <centroids_file> <dataset_file>")
        return

    centroids = np.loadtxt(centroids_f, float, delimiter=',')
    points = np.loadtxt(dataset_f, float, delimiter=',')

    cmap = cm.get_cmap('plasma')
    colors = cmap(np.linspace(0, 1, len(centroids)))

    pt_x = []
    pt_y = []
    pt_color = []

    c_x = []
    c_y = []

    for pt in points:
        pt_color.append(colors[closest_mean(pt, centroids)])
        pt_x.append(pt[0])
        pt_y.append(pt[1])

    for pt in centroids:
        c_x.append(pt[0])
        c_y.append(pt[1])

    plt.scatter(pt_x, pt_y, c=pt_color, s=1, alpha=1)
    plt.scatter(c_x, c_y, color='black', s=5, alpha=1)
    plt.show()


if __name__ == '__main__':
    main(*sys.argv)
