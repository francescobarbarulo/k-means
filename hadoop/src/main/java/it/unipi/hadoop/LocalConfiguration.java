package it.unipi.hadoop;

import java.io.File;
import java.io.IOException;
import org.apache.log4j.BasicConfigurator;
import org.ini4j.Wini;


public class LocalConfiguration {
    // [Dataset]
    private int numberOfDimensions;
    private int numberOfClusters;
    private String inputPath;
    private String outputPath;

    // [K-means]
    private int seedRNG;
    private int clusteringNumberOfReduceTasks;
    private double errorThreshold;
    private int maxNumberOfIterations;

    // [Hadoop]
    private boolean verbose;

    public LocalConfiguration(String configPath) {
        BasicConfigurator.configure();

        try{
            Wini config = new Wini(new File(configPath));

            numberOfDimensions = Integer.parseInt(config.get("Dataset", "numberOfDimensions"));
            numberOfClusters = Integer.parseInt(config.get("Dataset", "numberOfClusters"));
            inputPath = config.get("Dataset", "inputPath");
            outputPath = config.get("Dataset", "outputPath");
            seedRNG = Integer.parseInt(config.get("K-means", "seedRNG"));
            clusteringNumberOfReduceTasks = Integer.parseInt(config.get("K-means", "maxNumberOfReduceTasks"));
            errorThreshold = Double.parseDouble(config.get("K-means", "errorThreshold"));
            maxNumberOfIterations = Integer.parseInt(config.get("K-means", "maxNumberOfIterations"));
            verbose = Boolean.parseBoolean(config.get("Hadoop", "verbose"));

            validateConfiguration();
        } catch(IOException e){
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    private void validateConfiguration() {
        if (numberOfDimensions <= 0) {
            System.err.println("LocalConfiguration validation error: the number of dimensions must be greater than 0");
            System.exit(1);
        }

        if (numberOfClusters <= 0) {
            System.err.println("LocalConfiguration validation error: the number of clusters must be greater than 0");
            System.exit(1);
        }

        if (clusteringNumberOfReduceTasks <= 0) {
            System.err.println("LocalConfiguration validation error: the number of reduce tasks must be greater than 0");
            System.exit(1);
        }

        if (errorThreshold < 0) {
            System.err.println("LocalConfiguration validation error: the error threshold must be greater than or equal to 0");
            System.exit(1);
        }

        if (maxNumberOfIterations <= 0) {
            System.err.println("LocalConfiguration validation error: the number of iterations must be greater than 0");
            System.exit(1);
        }
    }

    public int getNumberOfDimensions() {
        return numberOfDimensions;
    }

    public int getNumberOfClusters() {
        return numberOfClusters;
    }

    public String getInputPath() {
        return inputPath;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public int getSeedRNG() {
        return seedRNG;
    }

    public int getClusteringNumberOfReducers() {
        return clusteringNumberOfReduceTasks;
    }

    public double getErrorThreshold() {
        return errorThreshold;
    }

    public int getMaxNumberOfIterations() {
        return maxNumberOfIterations;
    }

    public boolean getVerbose() {
        return verbose;
    }

    public void printConfiguration() {
        System.out.println("numberOfDimensions = " + numberOfDimensions);
        System.out.println("numberOfClusters = " + numberOfClusters);
        System.out.println("inputPath = " + inputPath);
        System.out.println("outputPath = " + outputPath);
        System.out.println("seedRNG = " + seedRNG);
        System.out.println("clusteringNumberOfReduceTasks = " + clusteringNumberOfReduceTasks);
        System.out.println("distanceThreshold = " + errorThreshold);
        System.out.println("maximumNumberOfIterations = " + maxNumberOfIterations);
        System.out.println("verbose = " + String.valueOf(verbose));
        System.out.println("");
    }
}
