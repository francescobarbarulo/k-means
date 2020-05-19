package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

public class kMeans {

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 5){
            System.err.println("Usage: hadoop jar target/kMeans-1.0-SNAPSHOT.jar it.unipi.hadoop.kMeans <n> <d> <k> <in> <out>");
            System.exit(1);
        }

        System.out.println("n=" + otherArgs[0]);
        System.out.println("d=" + otherArgs[1]);
        System.out.println("k=" + otherArgs[2]);
        System.out.println("input=" + otherArgs[3]);
        System.out.println("output=" + otherArgs[4]);

        conf.set("n", otherArgs[0]);                    // number of points
        conf.set("d", otherArgs[1]);                    // point space dimensions
        conf.set("k", otherArgs[2]);                    // number of means
        conf.set("input", otherArgs[3]);                // points
        conf.set("output", otherArgs[4]);               // centroids
        conf.set("sampledMeans", "sampled");            // sampled means
        conf.set("intermediateMeans", "tmp");           // tmp folder
        conf.set("error", "error");                     // convergence error
        conf.set("host", "falken-namenode:9820");
        conf.set("user", "hadoop");

        /* For local debugging */
        // conf.set("host", "localhost:9000");
        // conf.set("user", "Francesco");

        FileSystem hdfs = FileSystem.get(URI.create("hdfs://" + conf.get("host")), conf, conf.get("user"));

        /*  (*
            # Sampling
            - input : points
            - output: k sampled points
         */

        hdfs.delete(new Path(conf.get("sampledMeans")), true);

        Job sampling = Job.getInstance(conf, "sampling means");
        if ( !Sampling.main(sampling) ) System.exit(1);

        /* *) */

        int step = 0;
        double err = Double.POSITIVE_INFINITY;
        double prev_err;

        do {
            prev_err = err;

            Path srcPath = (step == 0) ? new Path(conf.get("sampledMeans") + "/part-r-00000") : new Path(conf.get("output") + "/part-r-00000");
            Path dstPath =  new Path(conf.get("intermediateMeans") + "/part-r-00000");
            FileUtil.copy(hdfs, srcPath, hdfs, dstPath, false, true, conf);

            /*
                (*
                # Clustering
                - input : points + intermediate means (hdfs)
                - output: new centroids
            */
            /* We can get rid of previous centroids because we are going to compute new ones */
            hdfs.delete(new Path(conf.get("output")), true);

            Job clustering = Job.getInstance(conf, "clustering");
            if ( !Clustering.main(clustering) ) System.exit(1);

            /* *) */

            /*  (*
                # Convergence:
                - input : points + actual centroids (hdfs)
                - output: convergence error
             */
            /* We can get rid of previous centroids because we are going to compute new ones */
            hdfs.delete(new Path(conf.get("error")), true);

            Job convergence = Job.getInstance(conf, "convergence");
            if ( !Convergence.main(convergence) ) System.exit(1);

            /* *) */

            /* (*  Read the convergence error  */

            FSDataInputStream fdsis = hdfs.open(new Path(conf.get("error") + "/part-r-00000"));
            BufferedReader br = new BufferedReader(new InputStreamReader(fdsis));

            String line;
            if ((line = br.readLine()) == null ) { System.exit(1); }

            err = Double.parseDouble(line);
            System.out.printf("\nSTEP: %d - PREV_ERR: %f - ERR: %f - CHANGE: %.2f%%\n\n", step, prev_err, err, (prev_err - err)/prev_err * 100);

            /* *) */

            step++;
        } while (prev_err == Double.POSITIVE_INFINITY || (prev_err - err)/prev_err > 0.01); // error changing in 1%
    }
}
