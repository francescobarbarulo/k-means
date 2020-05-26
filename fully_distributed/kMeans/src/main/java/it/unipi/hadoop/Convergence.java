
package it.unipi.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Convergence {
    
    public static class ConvergenceMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
        private static final DoubleWritable maximumDistance = new DoubleWritable();
        private static final IntWritable outputKey = new IntWritable(0);
        
        public void setup(Context context) {
            maximumDistance.set(Double.NEGATIVE_INFINITY);
        }
        
        public void map(LongWritable key, Text value, Context context) {
            double parsedDistance = Double.valueOf(value.toString());
            
            if (parsedDistance > maximumDistance.get())
                maximumDistance.set(parsedDistance);
        }
        
        public void cleanup(Context context) throws IOException, InterruptedException {
            context.write(outputKey, maximumDistance);
        }
    }
    
    public static class ConvergenceReducer extends Reducer<IntWritable, DoubleWritable, NullWritable, DoubleWritable> {
        private static final DoubleWritable maximumDistance = new DoubleWritable();
        
        public void setup(Context context) {
            maximumDistance.set(Double.NEGATIVE_INFINITY);
        }
        
        public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) {
            for (DoubleWritable distance : values) {
                if (distance.get() > maximumDistance.get())
                    maximumDistance.set(distance.get());
            }
        }
        
        public void cleanup(Context context) throws IOException, InterruptedException {
            context.write(null, maximumDistance);
        }
    }
    
    public static boolean main(Job job) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = job.getConfiguration();

        // Set JAR class.
        job.setJarByClass(Convergence.class);

        // Set Mapper class.
        job.setMapperClass(ConvergenceMapper.class);

        // Set Reducer class. It must be a single reducer.
        job.setReducerClass(ConvergenceReducer.class);
        job.setNumReduceTasks(1);
        
        // Set key-value output format.
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        // Define input and output path file.
        FileInputFormat.addInputPath(job, new Path(conf.get("clusteringFinalMeans") + "/" + conf.get("clusteringFinalMeans_DistanceBetweenMeans")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("convergence")));
        
        // Exit.
        return job.waitForCompletion(conf.getBoolean("verbose", true));
    } 
}
