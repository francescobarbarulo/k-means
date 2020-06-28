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
import java.util.PriorityQueue;
import java.util.Random;


public class Sampling {

    public static class SamplingMapper extends Mapper<LongWritable, Text, IntWritable, Point> {
        static int K;

        final static Random rand = new Random();
        final static IntWritable outputKey = new IntWritable();
        final static Point outputValue = new Point();

        /* In-Mapper Combiner: emit at most K points stored in the PriorityQueue */
        final static PriorityQueue<PriorityPoint> pq = new PriorityQueue<>();

        public void setup(Context context){
            Configuration conf = context.getConfiguration();
            K = Integer.parseInt(conf.get("k"));
            rand.setSeed(Long.parseLong(conf.get("seed")));
        }

        public void map(LongWritable key, Text value, Context context) {
            pq.add(new PriorityPoint(rand.nextInt(), value.toString()));

            /* Keep the queue size up to K */
            if (pq.size() > K)
                pq.poll();
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            /*
                key   : a random value
                value : the point
            */
            for (PriorityPoint pp: pq){
                outputKey.set(pp.getPriority());
                outputValue.set(pp.getCoordinates());
                context.write(outputKey, outputValue);
            }
        }
    }

    public static class SamplingReducer extends Reducer<IntWritable, Point, NullWritable, Point>{

        static int K;
        static int meansCount;

        public void setup(Context context){
            Configuration conf = context.getConfiguration();
            K = Integer.parseInt(conf.get("k"));

            meansCount = 0;
        }

        public void reduce(IntWritable key, Iterable<Point> values, Context context) throws IOException, InterruptedException {
            for (Point p: values){
                if (meansCount > K)
                    return;

                context.write(null, p);
                meansCount++;
            }
        }
    }

    public static boolean main(Job job) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = job.getConfiguration();

        // Set JAR class
        job.setJarByClass(Sampling.class);

        // Set Mapper class
        job.setMapperClass(SamplingMapper.class);

        // Set Reducer class
        job.setReducerClass(SamplingReducer.class);

        // Set key-value output format
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Point.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Point.class);

        // Define input and output path file
        FileInputFormat.addInputPath(job, new Path(conf.get("input")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("sampledMeans")));

        // Exit
        return job.waitForCompletion(true);
    }
}
