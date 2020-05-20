package it.unipi.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Scanner;


public class kMeans {
    
    private static void setupConfiguration(LocalConfiguration localConfig, Configuration conf) {
        // File system manipulation
        conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());

        // Parameters
        conf.set("numberOfPoints", localConfig.getNumberOfPoints());
        conf.set("numberOfDimensions", localConfig.getNumberOfDimensions());
        conf.set("numberOfClusters", localConfig.getNumberOfClusters());
        conf.set("inputPath", localConfig.getInputPath());
        conf.set("outputPath", localConfig.getOutputPath());
        conf.set("seedRNG", localConfig.getSeedRNG());
        conf.set("clusteringNumberOfReduceTasks", localConfig.getClusteringNumberOfReduceTasks());
        
        // Directories
        conf.set("meansElection", "means-election");
        conf.set("clusteringClosestPoints", "clustering-closest-points");
        conf.set("clusteringNewMeans", "clustering-new-means");
        
        //conf.set("intermediateMeans", "intermediate-means");
        //conf.set("finalMeans", "final-means");
    }
    
    private static void cleanWorkingDirectories(FileSystem hdfs, Configuration conf) throws IOException {
        hdfs.delete(new Path(conf.get("meansElection")), true);
        hdfs.delete(new Path(conf.get("clusteringClosestPoints")), true);
        hdfs.delete(new Path(conf.get("clusteringNewMeans")), true);
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        LocalConfiguration localConfig = new LocalConfiguration("config.ini");
        localConfig.printConfiguration();
        
        Configuration conf = new Configuration();
        setupConfiguration(localConfig, conf);
        
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://" + localConfig.getNamenode() + ":" + localConfig.getNamenodePort()), conf);
        cleanWorkingDirectories(hdfs, conf);
        
        // First step: select the initial random means and save them in the "startingMeans" directory.
        Job meansElection = Job.getInstance(conf, "means_election");
        if (!MeansElection.main(meansElection)) {
           System.err.println("****** ERROR: the means election failed. Exiting the job. ******\n");
           System.exit(1);
        }
        
        System.out.println("****** SUCCESS: the means election succeeded. ******\n");
        
        Job clusteringClosestPoints = Job.getInstance(conf, "clustering_closest_points");
        if (!Clustering_ClosestPoints.main(clusteringClosestPoints)) {
           System.err.println("****** ERROR: the clustering (closest points phase) iteration failed. Exiting the job. ******\n");
           System.exit(1);
        }
        
        System.out.println("****** SUCCESS: the clustering (closest points phase) iteration succeeded. ******\n");
        
        Job clusteringNewMeans = Job.getInstance(conf, "clustering_new_means");
        if (!Clustering_NewMeans.main(clusteringNewMeans)) {
           System.err.println("****** ERROR: the clustering (new means phase) iteration failed. Exiting the job. ******\n");
           System.exit(1);
        }
        
        System.out.println("****** SUCCESS: the clustering (new means phase) iteration succeeded. ******\n");
        
        /*double err = Double.POSITIVE_INFINITY;
        
        for (int i = 0; i < 3; i++) {
            System.out.print("=========================\n");
            System.out.printf("======== STEP %d ========\n", i);
            System.out.print("=========================\n\n");

            if (i == 0)
                // If it's the first step we take the sampled means
                FileUtils.copyDirectory(new File(conf.get("startingMeans")), new File(conf.get("intermediateMeans")));
            else
                //In the next steps we take the new centroids computed in the previous step
                FileUtils.copyDirectory(new File(conf.get("finalMeans")), new File(conf.get("intermediateMeans")));

            // We can get rid of previous centroids because we are going to compute new ones
            FileUtils.deleteDirectory(new File(conf.get("finalMeans")));

            Job clustering = Job.getInstance(conf, "clustering");
            clustering.addCacheFile(new Path(conf.get("intermediateMeans") + "/part-r-00000").toUri());
            boolean clusteringExit = Clustering.main(clustering);

            FileUtils.deleteDirectory(new File(conf.get("output")));

            Job convergence = Job.getInstance(conf, "convergence");
            convergence.addCacheFile(new Path(conf.get("finalMeans") + "/part-r-00000").toUri());
            boolean convergenceExit = Convergence.main(convergence);

            File f = new File(conf.get("output")+"/part-r-00000");
            Scanner sc = new Scanner(f);

            if ( !sc.hasNextLine() ) { System.exit(1); }

            err = Double.parseDouble(sc.nextLine());
            System.out.println("\n******ERR: " + err + "*********\n");
        }*/
    } 
}