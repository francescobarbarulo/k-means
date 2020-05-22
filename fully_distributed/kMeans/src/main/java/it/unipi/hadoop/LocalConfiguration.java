
package it.unipi.hadoop;

import java.io.File;
import java.io.IOException;
import org.apache.log4j.BasicConfigurator;
import org.ini4j.Wini;


public class LocalConfiguration {
    // [Connection]
    private String namenode;
    private String namenodePort; 
    
    // [Dataset]
    private long numberOfPoints;
    private int numberOfDimensions;
    private int numberOfClusters;
    private String inputPath;
    private String outputPath;
    
    // [K-means]
    private int seedRNG;
    private int clusteringNumberOfReduceTasks;
    private double errorThreshold;
    private int maximumNumberOfIterations;
    
    public LocalConfiguration(String configPath) {
        BasicConfigurator.configure();
        
        try{
            Wini config = new Wini(new File(configPath));
            
            namenode = config.get("Connection", "namenode");
            namenodePort = config.get("Connection", "port");
            numberOfPoints = Long.parseLong(config.get("Dataset", "numberOfPoints"));
            numberOfDimensions = Integer.parseInt(config.get("Dataset", "numberOfDimensions"));
            numberOfClusters = Integer.parseInt(config.get("Dataset", "numberOfClusters"));
            inputPath = config.get("Dataset", "inputPath");
            outputPath = config.get("Dataset", "outputPath");
            seedRNG = Integer.parseInt(config.get("K-means", "seedRNG"));
            clusteringNumberOfReduceTasks = Integer.parseInt(config.get("K-means", "numberOfReduceTasks"));
            errorThreshold = Double.parseDouble(config.get("K-means", "errorThreshold"));
            maximumNumberOfIterations = Integer.parseInt(config.get("K-means", "maximumNumberOfIterations"));
            
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
        
        if (errorThreshold < 0) { 
            System.err.println("LocalConfiguration validation error: the error threshold must be greater than or equal to 0");
            System.exit(1);
        }
        
        if (maximumNumberOfIterations <= 0) { 
            System.err.println("LocalConfiguration validation error: the number of iterations must be greater than 0");
            System.exit(1);
        }
    }

    public String getNamenode() {
        return namenode;
    }
    
    public String getNamenodePort() {
        return namenodePort;
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
    
    public double getErrorThreshold() {
        return errorThreshold;
    }
    
    public int getMaximumNumberOfIterations() {
        return maximumNumberOfIterations;
    }
     
    public void printConfiguration() {
        System.out.println("namenode = " + namenode);
        System.out.println("namenodePort = " + namenodePort);
        System.out.println("numberOfPoints = " + numberOfPoints);
        System.out.println("numberOfDimensions = " + numberOfDimensions);
        System.out.println("numberOfClusters = " + numberOfClusters);
        System.out.println("inputPath = " + inputPath);
        System.out.println("outputPath = " + outputPath);
        System.out.println("seedRNG = " + seedRNG);
        System.out.println("clusteringNumberOfReduceTasks = " + clusteringNumberOfReduceTasks);
        System.out.println("errorThreshold = " + errorThreshold + "%");
        System.out.println("maximumNumberOfIterations = " + maximumNumberOfIterations);
        System.out.println("");
    }
}
