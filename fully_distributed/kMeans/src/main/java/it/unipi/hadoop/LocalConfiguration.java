
package it.unipi.hadoop;

import java.io.File;
import java.io.IOException;
import org.apache.log4j.BasicConfigurator;
import org.ini4j.Wini;


public class LocalConfiguration {
    private String namenode;
    private String namenodePort; 
    private String numberOfPoints;
    private String numberOfDimensions;
    private String numberOfClusters;
    private String inputPath;
    private String outputPath;
    private String seedRNG;
    private String clusteringNumberOfReduceTasks;
    
    public LocalConfiguration(String configPath) {
        BasicConfigurator.configure();
        
        try{
            Wini config = new Wini(new File(configPath));
            
            namenode = config.get("Connection", "namenode");
            namenodePort = config.get("Connection", "port");
            numberOfPoints = config.get("Dataset", "numberOfPoints");
            numberOfDimensions = config.get("Dataset", "numberOfDimensions");
            numberOfClusters = config.get("Dataset", "numberOfClusters");
            inputPath = config.get("Dataset", "inputPath");
            outputPath = config.get("Dataset", "outputPath");
            seedRNG = config.get("InitialRandomMeans", "seedRNG");
            clusteringNumberOfReduceTasks = config.get("Clustering", "numberOfReduceTasks");
            
        } catch(IOException e){
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }
        
    public String getNamenode() {
        return namenode;
    }

    public String getNamenodePort() {
        return namenodePort;
    }

    public String getNumberOfPoints() {
        return numberOfPoints;
    }

    public String getNumberOfDimensions() {
        return numberOfDimensions;
    }

    public String getNumberOfClusters() {
        return numberOfClusters;
    }

    public String getInputPath() {
        return inputPath;
    }

    public String getOutputPath() {
        return outputPath;
    }
    
    public String getSeedRNG() {
        return seedRNG;
    }
    
    public String getClusteringNumberOfReduceTasks() {
        return clusteringNumberOfReduceTasks;
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
    }
}
