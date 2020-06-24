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

    public static double getConvergenceError() throws IOException {
        double err;
        InputStream is = fs.open(new Path(conf.get("convergence") + "/part-r-00000"));
        BufferedReader br = new BufferedReader(new InputStreamReader(is));

        String line;
        if ((line = br.readLine()) == null ) {
            br.close();
            fs.close();
            System.exit(1);
        }

        err = Double.parseDouble(line);
        br.close();
        return err;
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
            - output: k sampled points
         */

        Job sampling = Job.getInstance(conf, "sampling means");
        if ( !Sampling.main(sampling) ) {
            fs.close();
            System.exit(1);
        }

        /* *) */

        int step = 0;
        double err = Double.POSITIVE_INFINITY;
        double prev_err;

        do {
            prev_err = err;

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

            err = getConvergenceError();

            System.out.printf("\nSTEP: %d - PREV_ERR: %f - ERR: %f - CHANGE: %.2f%%\n\n", step, prev_err, err, (prev_err - err)/prev_err * 100);

            step++;
            System.out.println("step count: " + step);
        } while (prev_err == Double.POSITIVE_INFINITY
                || (100 * (prev_err - err)/prev_err > errorThreshold && step < maxNumberOfIterations));

        fs.close();

    }
}
