package it.unipi.kmeans.test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import org.ini4j.Wini;
import org.json.JSONArray;
import org.json.JSONObject;

public class Test {
    private static int[] numberOfPoints = {1000, 10000, 100000};
    private static int[] numberOfClusters = {7, 13};
    private static int[] numberOfDimensions = {3, 7};
    private static int repetitions = 10;
    
    private static String getLaunchCommand(String[] args) {
        if (args.length != 1) {
            System.out.println("Invalid format for the command line arguments");
            System.out.println("Usage: <hadoop/spark>");
            System.exit(1);
        }
        
        String programToTest = args[0].toLowerCase();
        String launchCommand = "";
        
        if (programToTest.equals("hadoop")) {
            launchCommand = "hadoop jar kMeans-1.0-SNAPSHOT-with_dependencies.jar it.unipi.hadoop.kMeans";
        } else if (programToTest.equals("spark")) {
            launchCommand = "spark-submit --master yarn --deploy-mode client --py-files util.zip main.py";
        } else {
            System.out.println("Invalid format for the command line arguments");
            System.out.println("Usage: <hadoop/spark>");
            System.exit(1);
        }
        
        launchCommand = launchCommand + " && echo done";
        System.out.println("Submitted command: " + launchCommand);
        return launchCommand;
    }
    
    private static void loadPointFile(String[] args, String pointFileName) throws IOException, InterruptedException {
        String programToTest = args[0].toLowerCase();
        Wini config = new Wini(new File("config.ini"));
        String inputPath = config.get("Dataset", "inputPath");
        
        // Delete the previous point file and load the new one
        Process deleteFile = Runtime.getRuntime().exec("hadoop fs -rm input/*");
        deleteFile.waitFor();
        
        Process copyFile = Runtime.getRuntime().exec("hadoop fs -put " + pointFileName + " input");
        copyFile.waitFor();
    }
    
    private static void modifyConfigurationFile(String[] args, int numberOfDimensions, int numberOfClusters, int seed) throws IOException {
        String programToTest = args[0].toLowerCase();
        Wini config = new Wini(new File("config.ini"));
        
        if (programToTest.equals("hadoop")) {
            config.put("Dataset", "numberOfDimensions", numberOfDimensions);
            config.put("Dataset", "numberOfClusters", numberOfClusters);
            config.put("K-means", "seedRNG", seed);
            config.put("K-means", "maxNumberOfReduceTasks", numberOfClusters);
            config.store();
        } else if (programToTest.equals("spark")) {            
            config.put("Dataset", "numberOfClusters", numberOfClusters);
            config.put("K-means", "seedRNG", seed);
            config.store();
        }
    }
    
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {     
        JSONArray results = new JSONArray();
        String launchCommand = getLaunchCommand(args);
        
        for (int i = 0; i < numberOfPoints.length; i++) {            
            for (int j = 0; j < numberOfDimensions.length; j++) {
                // Load the file with the correct number of points and dimension
                // File format: data_numberOfPoints_numberOfDimensions
                String pointFileName = "inputData/data_" + numberOfPoints[i] + "_" + numberOfDimensions[j] + ".txt";
                loadPointFile(args, pointFileName);
                
                for (int l = 0; l < numberOfClusters.length; l++) {
                    for (int r = 1; r <= repetitions; r++) {
                        System.out.print(args[0].toUpperCase() + "   ");
                        System.out.print("Stage: numberOfPoints = " + numberOfPoints[i] + ", ");
                        System.out.print("numberOfDimensions = " + numberOfDimensions[j] + ", ");
                        System.out.print("numberOfClusters = " + numberOfClusters[l] + ", ");
                        System.out.println("repetition = " + r);
                                
                        // Use r as seed for the RNG
                        modifyConfigurationFile(args, numberOfDimensions[j], numberOfClusters[l], r);
                        
                        long startTime = System.currentTimeMillis();
                        Process process = Runtime.getRuntime().exec(launchCommand);
                        
                        StreamConsumer errorConsumer = new StreamConsumer(process.getErrorStream(), "ERROR");
                        StreamConsumer inputConsumer = new StreamConsumer(process.getInputStream(), "OUTPUT");
                        errorConsumer.start();
                        inputConsumer.start();
                        
                        process.waitFor();
                        long endTime = System.currentTimeMillis();
                        
                        JSONObject iterationResult = new JSONObject();
                        iterationResult.put("executionTime", endTime - startTime);
                        iterationResult.put("numberOfPoints", numberOfPoints[i]);
                        iterationResult.put("numberOfDimensions", numberOfDimensions[j]);
                        iterationResult.put("numberOfClusters", numberOfClusters[l]);
                        iterationResult.put("repetition", r);
                        
                        results.put(iterationResult);
                        
                        // Intermediate print to file to backup values.
                        try (PrintWriter out = new PrintWriter("result_" + args[0].toLowerCase() + "_partial" + ".json")) {
                            out.println(results.toString(4));
                        } catch (Exception exception) {
                            exception.printStackTrace();
                            System.out.println(results.toString(4));
                        }  
                    }
                }
            }   
        }
        
        try (PrintWriter out = new PrintWriter("result_" + args[0].toLowerCase() + ".json")) {
            out.println(results.toString(4));
        } catch (Exception exception) {
            exception.printStackTrace();
            System.out.println(results.toString(4));
        }       
    }
}
