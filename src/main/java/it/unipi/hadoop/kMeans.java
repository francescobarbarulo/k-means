package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

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

        conf.set("n", otherArgs[0]);
        conf.set("d", otherArgs[1]);
        conf.set("k", otherArgs[2]);
        conf.set("input", otherArgs[3]);
        conf.set("output", otherArgs[4]);
        conf.set("intermediateOutput", "starting-means/");

        Job meansElection = Job.getInstance(conf, "means election");
        boolean meansElectionExit = MeansElection.main(meansElection);

        DistributedCache.addCacheFile(new Path(conf.get("intermediateOutput")+"part-r-00000").toUri(), conf);

        Job clustering = Job.getInstance(conf, "clustering");
        boolean clusteringExit = Clustering.main(clustering);

        System.exit((meansElectionExit && clusteringExit) ? 0 : 1);
    }
}
