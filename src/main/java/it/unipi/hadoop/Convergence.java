package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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

        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            distance = new HashMap<>();

            FileSystem hdfs = FileSystem.get(URI.create("hdfs://" + conf.get("host")), conf, conf.get("user"));
            FSDataInputStream fdsis = hdfs.open(new Path(conf.get("output") + "/part-r-00000"));
            BufferedReader br = new BufferedReader(new InputStreamReader(fdsis));

            String line;
            while ((line = br.readLine()) != null){
                Point mean = Point.parse(line);
                distance.put(mean, 0.0);
            }
        }

        public void map(LongWritable key, Text value, Context context) {
            double minDistance = Double.POSITIVE_INFINITY;
            Point closestMean = null;

            Point p = Point.parse(value.toString());

            for (Point m: distance.keySet()){
                double d = p.getDistance(m);
                if (d < minDistance){
                    minDistance = d;
                    closestMean = m;
                }
            }

            double currentDistance = distance.get(closestMean);
            distance.put(closestMean, currentDistance + minDistance);
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

        public void reduce(Point key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
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
        FileOutputFormat.setOutputPath(job, new Path(conf.get("error")));

        // Exit
        return job.waitForCompletion(true);
    }
}
