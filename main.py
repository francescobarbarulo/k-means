from kMeans.PointUtility import PointUtility
from kMeans.LocalConfiguration import LocalConfiguration
from pyspark import SparkContext


def delete_output_file(output_file, spark_context):
    URI = spark_context._gateway.jvm.java.net.URI
    Path = spark_context._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = spark_context._gateway.jvm.org.apache.hadoop.fs.FileSystem

    fileSystem = FileSystem.get(spark_context._jsc.hadoopConfiguration())
    fileSystem.delete(Path(output_file))


def get_random_means(config, points_rdd, spark_context):
    # The sample() method is probabilistic and does not ensure that exactly K values are returned, so the sampling is
    # executed multiple times if the required size is not met, with a fraction of points doubled at each iteration.
    fraction = config.get_number_of_clusters()/config.get_number_of_points()

    while True:
        sampled_means = points_rdd.sample(False, fraction, config.get_seed_RNG())
        sample_size = sampled_means.count()

        if sample_size == config.get_number_of_clusters():
            return sampled_means

        if sample_size > config.get_number_of_clusters():
            sampled_means = sampled_means.zipWithIndex().filter(lambda mean: mean[1] < config.get_number_of_clusters()).keys()
            return sampled_means

        fraction = fraction*2


def stop_condition(max_distance_between_means, iteration_number, distance_threshold):
    print("****** Iteration number: " + str(iteration_number + 1) + " ******")
    print("****** Maximum distance between old and new means: " + str(max_distance_between_means) + " ******")
    print("****** Distance threshold: " + str(distance_threshold) + " ******")

    if iteration_number == 0:
        print("****** First iteration: stop condition not checked. ******\n")
        return False

    if max_distance_between_means <= distance_threshold:
        print("****** Stop condition met: distance " + str(max_distance_between_means) + " ******\n")
        return True

    print()
    return False


def main():
    config = LocalConfiguration("config.ini")
    config.print()

    spark_context = SparkContext(appName="K-Means")
    spark_context.setLogLevel(config.get_log_level())

    # Remove output file from previous execution.
    delete_output_file(config.get_output_path() + "/final-means", spark_context)

    # Parse the points from txt files.
    points_rdd = spark_context.textFile(config.get_input_path()).map(PointUtility.parse_point).cache()

    # First step: select the initial random means.
    sampled_means = get_random_means(config, points_rdd, spark_context).cache()

    # Second step: update the means until a stop condition is met.
    iteration_means = sampled_means
    completed_iterations = 0

    while completed_iterations < config.get_maximum_number_of_iterations():
        # Compute an rdd of values in the format (old_mean, new_mean, distance), where "distance"
        # is the euclidean distance between each old/new mean couple.
        old_new_means_rdd = points_rdd.cartesian(iteration_means)\
                                        .map(PointUtility.cast_to_dictionary)\
                                        .reduceByKey(PointUtility.get_closest_mean)\
                                        .map(PointUtility.cast_to_tuple)\
                                        .reduceByKey(PointUtility.sum_partial_means)\
                                        .map(PointUtility.compute_new_mean)

        new_means_rdd = old_new_means_rdd.map(lambda x: x[1])
        max_distance_between_means = old_new_means_rdd.map(lambda x: x[2]).max()

        if stop_condition(max_distance_between_means, completed_iterations, config.get_distance_threshold()):
            new_means_rdd.map(PointUtility.to_string).saveAsTextFile(config.get_output_path() + "/final-means")
            spark_context.stop()
            return

        iteration_means.unpersist()
        iteration_means = new_means_rdd
        iteration_means.cache()

        completed_iterations += 1

    spark_context.stop()
    print("****** Maximum number of iterations reached: " + str(completed_iterations) + " ******")


if __name__ == "__main__":
    main()