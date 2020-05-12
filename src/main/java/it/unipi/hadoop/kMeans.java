package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Scanner;

public class kMeans {

    private static void deleteDir(File f){
        if (f.isDirectory()){
            File[] entries = f.listFiles();
            if (entries != null) {
                for (File e : entries) {
                    e.delete();
                }
            }
        }
        f.delete();
    }

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

        conf.set("n", otherArgs[0]);
        conf.set("d", otherArgs[1]);
        conf.set("k", otherArgs[2]);
        conf.set("input", otherArgs[3]);
        conf.set("output", otherArgs[4]);
        conf.set("startingMeans", "starting-means");
        conf.set("finalMeans", "final-means");

        double previousErr = Double.POSITIVE_INFINITY;
        double err = Double.POSITIVE_INFINITY;

        deleteDir(new File(conf.get("startingMeans")));
        deleteDir(new File(conf.get("finalMeans")));
        deleteDir(new File(conf.get("output")));

        Job meansElection = Job.getInstance(conf, "means election");
        boolean meansElectionExit = MeansElection.main(meansElection);

        for (int i = 0; i < 2; i++) {
            System.out.println("\n======== NEW STEP ========\n");

            previousErr = err;

            Job clustering = Job.getInstance(conf, "clustering");
            if (i == 0)
                clustering.addCacheFile(new Path(conf.get("startingMeans") + "/part-r-00000").toUri());
            else
                clustering.addCacheFile(new Path(conf.get("finalMeans") + "/part-r-00000").toUri());
            boolean clusteringExit = Clustering.main(clustering);

            Job convergence = Job.getInstance(conf, "convergence");
            if (i == 0)
                convergence.addCacheFile(new Path(conf.get("finalMeans") + "/part-r-00000").toUri());
            else
                convergence.addCacheFile(new Path(conf.get("finalMeans") + "/part-r-00000").toUri());
            boolean convergenceExit = Convergence.main(convergence);

            File f = new File(conf.get("output")+"/part-r-00000");
            Scanner sc = new Scanner(f);

            if ( !sc.hasNextLine() ) { System.exit(1); }

            err = Double.parseDouble(sc.nextLine());
            System.out.println("******ERR: " + err + "*********");
        }
    }
}
