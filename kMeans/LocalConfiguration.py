import configparser as cp


class LocalConfiguration:

    def __init__(self, configuration_path):
        self.configuration = cp.ConfigParser()
        self.configuration.read(configuration_path)

        # Dataset
        self.input_path = self.configuration.get("Dataset", "inputPath")
        self.output_path = self.configuration.get("Dataset", "outputPath")
        self.number_of_clusters = self.configuration["Dataset"].getint("numberOfClusters")
        self.number_of_points = self.configuration["Dataset"].getint("numberOfPoints")

        # K-means
        self.seed_RNG = self.configuration["K-means"].getint("seedRNG")
        self.distance_threshold = self.configuration["K-means"].getfloat("distanceThreshold")
        self.max_number_iterations = self.configuration["K-means"].getint("maximumNumberOfIterations")

        # Spark
        self.log_level = self.configuration.get("Spark", "logLevel")

        self.__validate()

    def __validate(self):
        if self.number_of_points <= 0:
            print("LocalConfiguration validation error: the number of points must be greater than 0")
            exit()

        if self.number_of_clusters <= 0:
            print("LocalConfiguration validation error: the number of clusters must be greater than 0")
            exit()

        if self.distance_threshold < 0:
            print("LocalConfiguration validation error: the distance threshold must be greater than or equal to 0")
            exit()

        if self.max_number_iterations <= 0:
            print("LocalConfiguration validation error: the number of iterations must be greater than 0")
            exit()

    def get_input_path(self):
        return self.input_path

    def get_output_path(self):
        return self.output_path

    def get_number_of_points(self):
        return self.number_of_points

    def get_number_of_clusters(self):
        return self.number_of_clusters

    def get_seed_RNG(self):
        return self.seed_RNG

    def get_log_level(self):
        return self.log_level

    def get_distance_threshold(self):
        return self.distance_threshold

    def get_maximum_number_of_iterations(self):
        return self.max_number_iterations

    def print(self):
        print("numberOfPoints = " + str(self.number_of_points))
        print("numberOfClusters = " + str(self.number_of_clusters))
        print("inputPath = " + self.input_path)
        print("outputPath = " + self.output_path)
        print("seedRNG = " + str(self.seed_RNG))
        print("distanceThreshold = " + str(self.distance_threshold) + "%")
        print("maximumNumberOfIterations = " + str(self.max_number_iterations))
        print("logLevel = " + self.log_level)