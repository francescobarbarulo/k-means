package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.Job;

import java.io.*;

public class kMeans {

    static Configuration conf;
    static FileSystem fs;

    static int maxNumberOfIterations;
    static double errorThreshold;

    public static void cleanWorkspace() throws IOException {
        fs.delete(new Path(conf.get("finalMeans")), true);
        fs.delete(new Path(conf.get("convergence")), true);
    }

    public static void copy(Path srcPath, Path dstPath) throws IOException {
        RemoteIterator<LocatedFileStatus> fileIter = fs.listFiles(srcPath, true);
        while (fileIter.hasNext()){
            FileUtil.copy(fs, fileIter.next(), fs, dstPath, false, true, conf);
        }
    }

    public static void addCacheDirectory(Path dir, Job job) throws IOException {
        RemoteIterator<LocatedFileStatus> fileIter = fs.listFiles(dir, true);
        while(fileIter.hasNext()) {
            job.addCacheFile(fileIter.next().getPath().toUri());
        }
    }

    public static double getObjectiveFunction() throws IOException {
        double objFunction;
        InputStream is = fs.open(new Path(conf.get("convergence") + "/part-r-00000"));
        BufferedReader br = new BufferedReader(new InputStreamReader(is));

        String line;
        if ((line = br.readLine()) == null ) {
            br.close();
            fs.close();
            System.exit(1);
        }

        objFunction = Double.parseDouble(line);
        br.close();
        return objFunction;
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        conf = new Configuration();
        LocalConfiguration localConfig = new LocalConfiguration("config.ini");
        localConfig.printConfiguration();

        maxNumberOfIterations = localConfig.getMaxNumberOfIterations();
        errorThreshold = localConfig.getErrorThreshold();

        String BASE_DIR = localConfig.getOutputPath() + "/";

        conf.setLong("seed", localConfig.getSeedRNG());
        conf.setInt("d", localConfig.getNumberOfDimensions());
        conf.setInt("k", localConfig.getNumberOfClusters());
        conf.setInt("maxNumberOfReduceTasks", localConfig.getClusteringNumberOfReducers());
        conf.set("input", localConfig.getInputPath());
        conf.set("sampledMeans", BASE_DIR + "sampled-means");
        conf.set("intermediateMeans", BASE_DIR + "intermediate-means");
        conf.set("finalMeans", BASE_DIR + "final-means");
        conf.set("convergence", BASE_DIR + "convergence");

        fs = FileSystem.get(conf);
        fs.delete(new Path(BASE_DIR), true);

        /*  (*
            # Sampling
            - input : points
            - output: k sampled points (sampled-means/)
         */

        Job sampling = Job.getInstance(conf, "sampling means");
        if ( !Sampling.main(sampling) ) {
            fs.close();
            System.exit(1);
        }

        /* *) */

        int step = 0;
        double objFunction = Double.POSITIVE_INFINITY;
        double prevObjFunction;
        double variation;

        do {
            prevObjFunction = objFunction;

            Path srcPath = (step == 0) ? new Path(conf.get("sampledMeans")) : new Path(conf.get("finalMeans"));
            Path dstPath = new Path(conf.get("intermediateMeans"));

            fs.mkdirs(dstPath);
            copy(srcPath, dstPath);

            cleanWorkspace();

            /*
                (*
                # Clustering
                - input : points + intermediate means (cache)
                - output: new centroids (final-means/)
            */

            Job clustering = Job.getInstance(conf, "clustering");
            addCacheDirectory(new Path(conf.get("intermediateMeans")), clustering);
            if ( !Clustering.main(clustering) ) {
                fs.close();
                System.exit(1);
            }

            /* *) */

            /*  (*
                # Convergence:
                - input : points + actual centroids (cache)
                - output: convergence error (convergence/)
             */

            Job convergence = Job.getInstance(conf, "convergence");
            addCacheDirectory(new Path(conf.get("finalMeans")), convergence);
            if ( !Convergence.main(convergence) ) {
                fs.close();
                System.exit(1);
            }

            /* *) */

            objFunction = getObjectiveFunction();

            variation = (prevObjFunction - objFunction)/prevObjFunction * 100;

            System.out.printf("\nSTEP: %d - PREV_OBJ_FUNCTION: %f - OBJ_FUNCTION: %f - CHANGE: %.2f%%\n\n", step, prevObjFunction, objFunction, variation);

            step++;
        } while (prevObjFunction == Double.POSITIVE_INFINITY
                || (variation > errorThreshold && step < maxNumberOfIterations));

        fs.close();

    }
}
