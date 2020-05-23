package it.unipi.hadoop;

import java.io.BufferedReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;


public class kMeans {
    private static FileSystem hdfs;
    
    
    private static void setupConfiguration(LocalConfiguration localConfig, Configuration conf) {
        // File system manipulation.
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());

        // Parameters.
        conf.setLong("numberOfPoints", localConfig.getNumberOfPoints());
        conf.setInt("numberOfDimensions", localConfig.getNumberOfDimensions());
        conf.setInt("numberOfClusters", localConfig.getNumberOfClusters());
        conf.set("inputPath", localConfig.getInputPath());
        conf.setInt("seedRNG", localConfig.getSeedRNG());
        conf.setInt("clusteringNumberOfReduceTasks", localConfig.getClusteringNumberOfReduceTasks());
        conf.setDouble("distanceThreshold", localConfig.getDistanceThreshold());
        conf.setBoolean("verbose", localConfig.getVerbose());
        
        // Working directories, based on the given output path.        
        conf.set("meansElection", localConfig.getOutputPath() + "/" + "means-election");
        conf.set("iterationMeans", localConfig.getOutputPath() + "/" + "iteration-means");
        conf.set("clusteringClosestPoints", localConfig.getOutputPath() + "/" + "clustering-closest-points");
        
        conf.set("clusteringNewMeans", localConfig.getOutputPath() + "/" + "clustering-new-means");
        conf.set("clusteringNewMeans_NewMeans", "new-means"); // Sub-directory of clusteringNewMeans.
        conf.set("clusteringNewMeans_DistanceBetweenMeans", "distance-between-means"); // Sub-directory of clusteringNewMeans.
        
        conf.set("convergence", localConfig.getOutputPath() + "/" + "convergence");
    }
    
    private static void createDirectoryWithinHDFS(String directoryPath) throws IOException {
        hdfs.mkdirs(new Path(directoryPath));
    }
        
    private static void deleteDirectoryWithinHDFS(String directoryPath) throws IOException {
        hdfs.delete(new Path(directoryPath), true);
    }
        
    private static void copyDirectoryFilesWithinHDFS(String sourceDirectory, String destinationDirectory, Configuration conf) throws IOException {
        RemoteIterator<LocatedFileStatus> sourceFiles = hdfs.listFiles(new Path(sourceDirectory), true);
        Path destinationPath = new Path(destinationDirectory);
        
        if(sourceFiles != null) {
            while(sourceFiles.hasNext()){
                FileUtil.copy(hdfs, sourceFiles.next().getPath(), hdfs, destinationPath, true, conf);
            }           
        }
    }
    
    private static void cleanWorkspace(Configuration conf) throws IOException {
        deleteDirectoryWithinHDFS(conf.get("meansElection"));
        deleteDirectoryWithinHDFS(conf.get("iterationMeans"));
        deleteDirectoryWithinHDFS(conf.get("clusteringClosestPoints"));
        deleteDirectoryWithinHDFS(conf.get("clusteringNewMeans"));
        deleteDirectoryWithinHDFS(conf.get("convergence"));
        
        // Create new iteration means directory.
        createDirectoryWithinHDFS(conf.get("iterationMeans"));
    }

    private static double parseMaximumDistanceBetweenMeans(Configuration conf) throws IOException {
        double maximumDistanceBetweenMeans = 0;
        
        // Single maximum distance inside a single file. Guaranteed by the single reducer for convergence.
        FSDataInputStream hdfsDataInputStream = hdfs.open(new Path(conf.get("convergence") + "/part-r-00000"));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(hdfsDataInputStream));
        String line = "";

        // It is a one line only file.
        while ((line = bufferedReader.readLine()) != null) {
            maximumDistanceBetweenMeans = Double.parseDouble(line);
        }
        
        bufferedReader.close();
        
        return maximumDistanceBetweenMeans;
    }
    
    private static void executeMeansElection(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {
        Job meansElection = Job.getInstance(conf, "means_election");
        
        if (!MeansElection.main(meansElection)) {
           System.err.println("****** ERROR: the means election failed. Exiting the job. ******\n");
           hdfs.close();
           System.exit(1);
        }
        
        System.out.println("****** SUCCESS: the means election succeeded. ******\n");
    }
    
    private static void executeKMeansIteration(Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {
        Job clusteringClosestPoints = Job.getInstance(conf, "clustering_closest_points"); 
        if (!Clustering_ClosestPoints.main(clusteringClosestPoints)) {
            System.err.println("****** ERROR: the clustering (closest points phase) iteration failed. Exiting the job. ******\n");
            hdfs.close();
            System.exit(1);
        }   
        System.out.println("****** SUCCESS: the clustering (closest points phase) iteration succeeded. ******\n");
            
            
        Job clusteringNewMeans = Job.getInstance(conf, "clustering_new_means");
        if (!Clustering_NewMeans.main(clusteringNewMeans)) {
            System.err.println("****** ERROR: the clustering (new means phase) iteration failed. Exiting the job. ******\n");
            hdfs.close();
            System.exit(1);
        }
        System.out.println("****** SUCCESS: the clustering (new means phase) iteration succeeded. ******\n");

          
        Job convergence = Job.getInstance(conf, "convergence");
        if (!Convergence.main(convergence)) {
            System.err.println("****** ERROR: the convergence iteration failed. Exiting the job. ******\n");
            hdfs.close();
            System.exit(1);
        }
        System.out.println("****** SUCCESS: the convergence iteration succeeded. ******\n");
    }
    
    private static boolean stopConditionMet(Configuration conf, int iterationNumber) throws IOException {
        double currentMaximumDistanceBetweenMeans = parseMaximumDistanceBetweenMeans(conf);
        double distanceThreshold = conf.getDouble("distanceThreshold", 1);
        
        System.out.println("****** Iteration number: " + (iterationNumber + 1) + " ******");
        System.out.println("****** Maximum distance between old and new means: " + currentMaximumDistanceBetweenMeans + " ******");
        System.out.println("****** Distance threshold: " + distanceThreshold + " ******\n");
       
        if (iterationNumber == 0) {
            System.out.println("****** First iteration: stop condition not checked. ******");
            return false;
        }
        
        if (currentMaximumDistanceBetweenMeans <= distanceThreshold ) {
            System.out.println("****** Stop condition met: distance " + currentMaximumDistanceBetweenMeans + " ******\n");
            return true;
        }
        
        return false;
    }
    
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        LocalConfiguration localConfig = new LocalConfiguration("config.ini");
        localConfig.printConfiguration();
        
        Configuration conf = new Configuration();
        setupConfiguration(localConfig, conf);
        
        hdfs = FileSystem.get(URI.create("hdfs://" + localConfig.getNamenode() + ":" + localConfig.getNamenodePort()), conf);
        cleanWorkspace(conf);
        
        // First step: select the initial random means.
        executeMeansElection(conf);
        copyDirectoryFilesWithinHDFS(conf.get("meansElection"), conf.get("iterationMeans"), conf);
        
        // Second step: update the means until a stop condition is met.
        int completedIterations = 0;
        
        while (completedIterations < localConfig.getMaximumNumberOfIterations()) {
            executeKMeansIteration(conf);
 
            if (stopConditionMet(conf, completedIterations)) {
                hdfs.close();
                return;
            }
            
            deleteDirectoryWithinHDFS(conf.get("iterationMeans"));
            createDirectoryWithinHDFS(conf.get("iterationMeans"));
            copyDirectoryFilesWithinHDFS(conf.get("clusteringNewMeans") + "/" + conf.get("clusteringNewMeans_NewMeans"), conf.get("iterationMeans"), conf);
            
            deleteDirectoryWithinHDFS(conf.get("clusteringClosestPoints"));
            deleteDirectoryWithinHDFS(conf.get("clusteringNewMeans"));
            deleteDirectoryWithinHDFS(conf.get("convergence"));

            completedIterations++;
        }
        
        System.out.println("****** Maximum number of iterations reached: " + completedIterations + " ******");
        hdfs.close();
    }
}