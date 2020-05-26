package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.Random;


public class MeansSampling {

    public static class MeansSamplingMapper extends Mapper<LongWritable, Text, IntWritable, Point> {
        private final static Random randomGenerator = new Random();
        private final static IntWritable outputKey = new IntWritable();
        private final static Point outputValue = new Point();
        
        public void setup(Context context){
            Configuration conf = context.getConfiguration();
            randomGenerator.setSeed(conf.getInt("seedRNG", 1));
        }  

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            outputKey.set(randomGenerator.nextInt());
            outputValue.set(Point.parse(value.toString()));
            
            if (outputValue.getNumberOfDimensions() != conf.getInt("numberOfDimensions", -1)) {
                System.err.println("The point " + outputValue.toString() + " does not match the configured number of dimensions. It has been excluded from the iteration.");
                System.err.println("Point dimensions: " + outputValue.getNumberOfDimensions() + "; configured dimensions: " + conf.getInt("numberOfDimensions", -1));
                return;
            }
            
            context.write(outputKey, outputValue);
        }
    }

    public static class MeansSamplingCombiner extends Reducer<IntWritable, Point, IntWritable, Point> {
        private static int meansCount;

        public void setup(Context context){
            meansCount = 0;
        }   
        
        public void reduce(IntWritable key, Iterable<Point> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int numberOfClusters = conf.getInt("numberOfClusters", 1);
            
            for (Point candidateMean : values){
                if (meansCount < numberOfClusters){
                    context.write(key, candidateMean);
                    meansCount++;
                } else
                    return;
            }
        }
    }
    
    public static class MeansSamplingReducer extends Reducer<IntWritable, Point, NullWritable, Point>{
        private static int meansCount;
        private static final Point chosenMean = new Point();

        public void setup(Context context){
            meansCount = 0;
        }

        public void reduce(IntWritable key, Iterable<Point> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int numberOfClusters = conf.getInt("numberOfClusters", 1);
            
            for (Point candidateMean : values){
                if (meansCount < numberOfClusters){
                    // Id of the means must go from 1 to numberOfClusters.
                    chosenMean.set((double[]) candidateMean.getCoordinates().get(), PointType.MEAN, meansCount + 1);
                    context.write(null, chosenMean);
                    meansCount++;
                } else 
                    return;  
            }
        }
    }

    public static boolean main(Job job) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = job.getConfiguration();

        // Set JAR class.
        job.setJarByClass(MeansSampling.class);

        // Set Mapper class.
        job.setMapperClass(MeansSamplingMapper.class);

        // Set Combiner class.
        job.setCombinerClass(MeansSamplingCombiner.class);

        // Set Reducer class. It must be a single reducer.
        job.setReducerClass(MeansSamplingReducer.class);
        job.setNumReduceTasks(1);

        // Set key-value output format.
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Point.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Point.class);

        // Define input and output path file.
        FileInputFormat.addInputPath(job, new Path(conf.get("inputPath")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("sampledMeans")));

        // Exit.
        return job.waitForCompletion(conf.getBoolean("verbose", true));
    }  
}