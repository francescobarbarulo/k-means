from kMeans.PointUtility import PointUtility
from kMeans.LocalConfiguration import LocalConfiguration
from pyspark import SparkContext


def delete_output_file(output_file, spark_context):
    URI = spark_context._gateway.jvm.java.net.URI
    Path = spark_context._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = spark_context._gateway.jvm.org.apache.hadoop.fs.FileSystem

    fileSystem = FileSystem.get(spark_context._jsc.hadoopConfiguration())
    fileSystem.delete(Path(output_file))


def stop_condition(objective_function, last_objective_function, iteration_number, error_threshold):
    print("****** Iteration number: " + str(iteration_number + 1) + " ******")
    print("****** Last objective function value: " + str(last_objective_function) + " ******")
    print("****** Current objective function value: " + str(objective_function) + " ******")

    if iteration_number == 0:
        print("****** First iteration: stop condition not checked. ******\n")
        return False

    error = 100*(abs(last_objective_function - objective_function)/last_objective_function)

    print("****** Current error: " + str(error) + "% ******")
    print("****** Error threshold: " + str(error_threshold) + "% ******\n")

    if error <= error_threshold:
        print("****** Stop condition met: error " + str(error) + "% ******\n")
        return True

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
    sampled_means = points_rdd.takeSample(False, config.get_number_of_clusters(), config.get_seed_RNG())

    # Second step: update the means until a stop condition is met.
    iteration_means = spark_context.broadcast(sampled_means)
    last_objective_function = float("inf")
    completed_iterations = 0

    while completed_iterations < config.get_maximum_number_of_iterations():
        partial_means_rdd = points_rdd.map(lambda point: PointUtility.get_closest_mean(point, iteration_means.value)).reduceByKey(lambda x, y: PointUtility.sum_partial_means(x, y))
        new_means = PointUtility.compute_new_means(partial_means_rdd.collect())

        # Broadcast the new means and compute the value of the objective function to minimize.
        iteration_means = spark_context.broadcast(new_means)
        objective_function = points_rdd.map(lambda point: PointUtility.compute_min_squared_distance(point, iteration_means.value)).sum()

        if stop_condition(objective_function, last_objective_function, completed_iterations, config.get_error_threshold()):
            spark_context.parallelize(new_means).map(PointUtility.to_string).saveAsTextFile(config.get_output_path() + "/final-means")
            spark_context.stop()
            return

        last_objective_function = objective_function
        completed_iterations += 1

    spark_context.stop()
    print("****** Maximum number of iterations reached: " + str(completed_iterations) + " ******")


if __name__ == "__main__":
    main()