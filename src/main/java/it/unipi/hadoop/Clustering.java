package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class Clustering {
    public static class ClusteringMapper extends Mapper<LongWritable, Text, Point, Point> {

        static List<Point> startingMeans;

        protected void setup(Context context) throws FileNotFoundException {
            startingMeans = new ArrayList<>();

            File means = new File(context.getConfiguration().get("intermediateMeans")+"/part-r-00000");
            Scanner sc = new Scanner(means);
            while (sc.hasNextLine()){
                startingMeans.add(Point.parse(sc.nextLine()));
            }

            System.out.println("\n***INTERMEDIATE MEANS***");
            System.out.println(startingMeans.toString() + "\n");
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            double minDistance = Double.POSITIVE_INFINITY;
            int meanIndex = -1;

            Point p = Point.parse(value.toString());

            for (Point m: startingMeans){
                double d = p.getDistance(m);
                if (d < minDistance){
                    minDistance = d;
                    meanIndex = startingMeans.indexOf(m);
                }
            }

            context.write(startingMeans.get(meanIndex), p);
        }
    }

    public static class ClusteringReducer extends Reducer<Point, Point, NullWritable, Point> {

        public void setup(Context context){
            System.out.println("\n*** CENTROIDS ***");
        }

        public void reduce(Point key, Iterable<Point> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();

            double[] coordinates = new double[Integer.parseInt(conf.get("d"))];
            Arrays.fill(coordinates, 0);

            Point centroid = new Point(coordinates);
            int n = 0;

            for (Point p: values){
                centroid.add(p);
                n++;
            }
            centroid.div(n);

            System.out.print(centroid + " ");
            context.write(null, centroid);
        }

        public void cleanup(Context context){
            System.out.println("\n");
        }
    }

    public static boolean main(Job job) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = job.getConfiguration();

        // Set JAR class
        job.setJarByClass(Clustering.class);

        // Set Mapper class
        job.setMapperClass(ClusteringMapper.class);

        // Set Reducer class
        job.setReducerClass(ClusteringReducer.class);

        // Set key-value output format
        job.setMapOutputKeyClass(Point.class);
        job.setMapOutputValueClass(Point.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Point.class);

        // Define input and output path file
        FileInputFormat.addInputPath(job, new Path(conf.get("input")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("finalMeans")));

        // Exit
        return job.waitForCompletion(true);
    }
}
