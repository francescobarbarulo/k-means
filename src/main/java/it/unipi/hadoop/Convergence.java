package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class Convergence {
    public static class ConvergenceMapper extends Mapper<LongWritable, Text, Point, DoubleWritable> {

        Map<Point, Double> distance;
        final static DoubleWritable outputValue = new DoubleWritable();

        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();

            /*
                Prepare the hashmap for the in-mapper combiner.
                The hashmap will contain an entry for each final centroid as key,
                and the summation of the distances between it and the closest points
                that belong to the cluster:
                { mean: distance }
             */

            distance = new HashMap<>();

            URI[] cacheFiles = context.getCacheFiles();
            FileSystem fs = FileSystem.get(conf);

            for (URI f: cacheFiles) {
                System.out.println(f.toString());
                InputStream is = fs.open(new Path(f));
                BufferedReader br = new BufferedReader(new InputStreamReader(is));

                String line;
                while ((line = br.readLine()) != null) {
                    Point mean = new Point(line);
                    distance.put(mean, 0.0);
                }

                br.close();
            }
        }

        public void map(LongWritable key, Text value, Context context) {
            /*
                The mapper gets a point and search for the closest mean.
                Then it sums the distance between them to the
                current value of the distance.
                The mapper will emit every mean with the sum of the distances
                with the point of the cluster:
                {
                    key: mean
                    value: distance
                }
             */

            double minDistance = Double.POSITIVE_INFINITY;
            Point closestMean = null;

            Point p = new Point(value.toString());

            for (Point m: distance.keySet()){
                double d = p.getDistance(m);
                if (d < minDistance){
                    minDistance = d;
                    closestMean = m;
                }
            }

            distance.put(closestMean, distance.get(closestMean) + minDistance);
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Point, Double> entry: distance.entrySet()){
                outputValue.set(entry.getValue());
                context.write(entry.getKey(), outputValue);
            }
        }
    }

    public static class ConvergenceReducer extends Reducer<Point, DoubleWritable, NullWritable, DoubleWritable> {

        static double sum;
        final static DoubleWritable outputValue = new DoubleWritable();

        public void setup(Context context){
            sum = 0.0;
        }

        public void reduce(Point key, Iterable<DoubleWritable> values, Context context) {
            /*
                The reducer sums up all the distances related to a mean got by key.
                The sum represents the convergence error.
             */
            for (DoubleWritable value: values)
                sum += value.get();
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            outputValue.set(sum);
            context.write(null, outputValue);
        }
    }

    public static boolean main(Job job) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = job.getConfiguration();

        // Set JAR class
        job.setJarByClass(Convergence.class);

        // Set Mapper class
        job.setMapperClass(ConvergenceMapper.class);

        // Set Reducer class
        job.setReducerClass(ConvergenceReducer.class);

        // Set key-value output format
        job.setMapOutputKeyClass(Point.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Define input and output path file
        FileInputFormat.addInputPath(job, new Path(conf.get("input")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("convergence")));

        // Exit
        return job.waitForCompletion(true);
    }
}
