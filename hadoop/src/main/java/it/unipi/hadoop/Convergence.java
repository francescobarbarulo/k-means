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
    public static class ConvergenceMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        List<Point> means;
        final static DoubleWritable outputValue = new DoubleWritable();
        static double error_accumulator;

        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            error_accumulator = (double) 0.0;
            /*
                prepare the list of means so that using the list we
                can find the distance a point to the nearest mean.
             */

            means = new ArrayList<Point>();

            URI[] cacheFiles = context.getCacheFiles();
            FileSystem fs = FileSystem.get(conf);

            for (URI f: cacheFiles) {
                System.out.println(f.toString());
                InputStream is = fs.open(new Path(f));
                BufferedReader br = new BufferedReader(new InputStreamReader(is));

                String line;
                while ((line = br.readLine()) != null) {
                    Point mean = new Point(line);
                    means.add(mean);
                }
                br.close();
            }
        }

        public void map(LongWritable key, Text value, Context context) {
            /*
                The mapper gets a point and search for the closest mean.
                Then it sums the distance between them to the
                current value of the distance that is accumulated for
                all previous points.
             */

            double minDistance = Double.POSITIVE_INFINITY;
            Point closestMean = null;

            Point p = new Point(value.toString());

            for (Point m: means){
                double d = p.getSquaredDistance(m);
                if (d < minDistance){
                    minDistance = d;
                }
            }

            error_accumulator += minDistance;
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            /*
            * export the sums of the distances using a comon key for the reducer.
            * */
            Text outKey = new Text("common_key");
            context.write(outKey, new DoubleWritable(error_accumulator));

        }
    }

    public static class ConvergenceReducer extends Reducer<Text, DoubleWritable, NullWritable, DoubleWritable> {

        static double sum;
        final static DoubleWritable outputValue = new DoubleWritable();

        public void setup(Context context){
            sum = 0.0;
        }

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) {
            /*
                The reducer sums up all the distances that it got from the mappers
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
        job.setMapOutputKeyClass(Text.class);
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
