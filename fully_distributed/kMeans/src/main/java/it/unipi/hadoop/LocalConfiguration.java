
package it.unipi.hadoop;

import java.io.File;
import java.io.IOException;
import org.apache.log4j.BasicConfigurator;
import org.ini4j.Wini;


public class LocalConfiguration {    
    // [Dataset]
    private long numberOfPoints;
    private int numberOfDimensions;
    private int numberOfClusters;
    private String inputPath;
    private String outputPath;
    
    // [K-means]
    private int seedRNG;
    private int clusteringNumberOfReduceTasks;
    private double distanceThreshold;
    private int maximumNumberOfIterations;
    
    // [Hadoop]
    private boolean verbose;
    
    public LocalConfiguration(String configPath) {
        BasicConfigurator.configure();
        
        try{
            Wini config = new Wini(new File(configPath));

            numberOfPoints = Long.parseLong(config.get("Dataset", "numberOfPoints"));
            numberOfDimensions = Integer.parseInt(config.get("Dataset", "numberOfDimensions"));
            numberOfClusters = Integer.parseInt(config.get("Dataset", "numberOfClusters"));
            inputPath = config.get("Dataset", "inputPath");
            outputPath = config.get("Dataset", "outputPath");
            seedRNG = Integer.parseInt(config.get("K-means", "seedRNG"));
            clusteringNumberOfReduceTasks = Integer.parseInt(config.get("K-means", "numberOfReduceTasks"));
            distanceThreshold = Double.parseDouble(config.get("K-means", "distanceThreshold"));
            maximumNumberOfIterations = Integer.parseInt(config.get("K-means", "maximumNumberOfIterations"));
            verbose = Boolean.valueOf(config.get("Hadoop", "verbose"));
            
            validateConfiguration();
        } catch(IOException e){
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }
    
    private void validateConfiguration() {
        if (numberOfPoints <= 0) { 
            System.err.println("LocalConfiguration validation error: the number of points must be greater than 0");
            System.exit(1);
        }
        
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
        
        if (distanceThreshold < 0) { 
            System.err.println("LocalConfiguration validation error: the error threshold must be greater than or equal to 0");
            System.exit(1);
        }
        
        if (maximumNumberOfIterations <= 0) { 
            System.err.println("LocalConfiguration validation error: the number of iterations must be greater than 0");
            System.exit(1);
        }
    }

    public long getNumberOfPoints() {
        return numberOfPoints;
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
    
    public int getClusteringNumberOfReduceTasks() {
        return clusteringNumberOfReduceTasks;
    }
    
    public double getDistanceThreshold() {
        return distanceThreshold;
    }
    
    public int getMaximumNumberOfIterations() {
        return maximumNumberOfIterations;
    }
    
    public boolean getVerbose() {
        return verbose;
    }
     
    public void printConfiguration() {
        System.out.println("numberOfPoints = " + numberOfPoints);
        System.out.println("numberOfDimensions = " + numberOfDimensions);
        System.out.println("numberOfClusters = " + numberOfClusters);
        System.out.println("inputPath = " + inputPath);
        System.out.println("outputPath = " + outputPath);
        System.out.println("seedRNG = " + seedRNG);
        System.out.println("clusteringNumberOfReduceTasks = " + clusteringNumberOfReduceTasks);
        System.out.println("distanceThreshold = " + distanceThreshold);
        System.out.println("maximumNumberOfIterations = " + maximumNumberOfIterations);
        System.out.println("verbose = " + String.valueOf(verbose));
        System.out.println("");
    }
}
