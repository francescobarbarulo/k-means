package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class Convergence {
    public static class ConvergenceMapper extends Mapper<LongWritable, Text, Point, DoubleWritable> {

        static List<Point> finalMeans;
        final static DoubleWritable outputValue = new DoubleWritable();

        protected void setup(Context context) throws FileNotFoundException {
            finalMeans = new ArrayList<>();

            File means = new File(context.getConfiguration().get("finalMeans")+"/part-r-00000");
            Scanner sc = new Scanner(means);
            while (sc.hasNextLine()){
                finalMeans.add(Point.parse(sc.nextLine()));
            }

            System.out.println("\n***FINAL MEANS***");
            System.out.println(finalMeans.toString() + "\n");
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            double minDistance = Double.POSITIVE_INFINITY;

            Point p = Point.parse(value.toString());

            for (Point m: finalMeans){
                double d = p.getDistance(m);
                if (d < minDistance){
                    minDistance = d;
                }
            }

            outputValue.set(minDistance);
            context.write(p, outputValue);
        }
    }

    public static class ConvergenceReducer extends Reducer<Point, DoubleWritable, NullWritable, DoubleWritable> {

        static double sum;
        final static DoubleWritable outputValue = new DoubleWritable();

        public void setup(Context context){
            sum = 0.0;
        }

        public void reduce(Point key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            for (DoubleWritable v: values)
                sum += v.get();
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
        FileOutputFormat.setOutputPath(job, new Path(conf.get("output")));

        // Exit
        return job.waitForCompletion(true);
    }
}
