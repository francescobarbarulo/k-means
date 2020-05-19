package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.util.*;

public class Clustering {
    public static class ClusteringMapper extends Mapper<LongWritable, Text, Point, Entry> {

        static int D;

        static Map<Point, Entry> centroidSummation;

        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            D = Integer.parseInt(conf.get("d"));

            centroidSummation = new HashMap<>();

            FileSystem hdfs = FileSystem.get(URI.create("hdfs://" + conf.get("host")), conf, conf.get("user"));
            FSDataInputStream fdsis = hdfs.open(new Path(conf.get("intermediateMeans") + "/part-r-00000"));
            BufferedReader br = new BufferedReader(new InputStreamReader(fdsis));

            String line;
            while ((line = br.readLine()) != null){
                Point mean = Point.parse(line);
                centroidSummation.put(mean, new Entry(new Point(D)));
            }
        }

        public void map(LongWritable key, Text value, Context context) {
            double minDistance = Double.POSITIVE_INFINITY;
            Point closestMean = null;

            Point p = Point.parse(value.toString());

            for (Point m: centroidSummation.keySet()){
                double d = p.getDistance(m);
                if (d < minDistance){
                    minDistance = d;
                    closestMean = m;
                }
            }

            Entry e = centroidSummation.get(closestMean);
            Point currentCentroidSummation = e.getPoint();
            int currentValueSummation = e.getValue();

            currentCentroidSummation.add(p);

            e.setPoint(currentCentroidSummation);
            e.setValue(currentValueSummation + 1);
            centroidSummation.put(closestMean, e);
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Point, Entry> entry: centroidSummation.entrySet()){
                context.write(entry.getKey(), entry.getValue());
            }
        }
    }

    public static class ClusteringReducer extends Reducer<Point, Entry, NullWritable, Point> {

        static int D;

        public void setup(Context context){
            Configuration conf = context.getConfiguration();
            D = Integer.parseInt(conf.get("d"));
        }

        public void reduce(Point key, Iterable<Entry> values, Context context) throws IOException, InterruptedException {
            Point centroid = new Point(D);
            int n = 0;

            for (Entry e: values){
                centroid.add(e.getPoint());
                n += e.getValue();
            }
            centroid.div(n);

            context.write(null, centroid);
        }
    }

    public static boolean main(Job job) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = job.getConfiguration();
        int K = Integer.parseInt(conf.get("k"));

        job.setJarByClass(Clustering.class);

        job.setMapperClass(ClusteringMapper.class);
        job.setReducerClass(ClusteringReducer.class);

        /*
            TODO -- We can have one reducer per cluster
            This means that we have multiple part-r-* files in output and we have to manage them.
        */
        // job.setNumReduceTasks(K);

        job.setMapOutputKeyClass(Point.class);
        job.setMapOutputValueClass(Entry.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Point.class);

        // Define input and output path file
        FileInputFormat.addInputPath(job, new Path(conf.get("input")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("output")));

        // Exit
        return job.waitForCompletion(true);
    }
}
