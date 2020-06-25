# Overview

This script is useful for generating a dataset of points both in a randomly and in a clusterable manner.

## Usage

Run the script by:
```bash
python main.py <number_of_points> <number_of_dimension> [<type>]
```

The `type` optional parameter can be:
- _random_: generates totally random point (default, it can be omitted)
- _cluster_: generates clusterable points, i.e. 3 clusters with `number_of_points` points each are created.

The output is a file containing one point per line represented by coordinates which are separated by comma.

The result file name is:

```
<number_of_points>-<number_of_dimensions>d-points.txt
```
