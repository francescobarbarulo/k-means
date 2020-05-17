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
        /*
            N : total number of points
            K : number of clusters
         */
        static int N, K;

        final static Random rand = new Random(0);
        final static IntWritable outputKey = new IntWritable();

        /* In-Mapper Combiner: emit at most K points stored in the PriorityQueue */
        static PriorityQueue<Entry> pq = new PriorityQueue<>();

        public void setup(Context context){
            Configuration conf = context.getConfiguration();
            N = Integer.parseInt(conf.get("n"));
            K = Integer.parseInt(conf.get("k"));
        }

        public void map(LongWritable key, Text value, Context context) {
            Entry e = new Entry(rand.nextInt(N), Point.parse(value.toString()));
            pq.add(e);

            /* Keep the queue size up to K */
            if (pq.size() > K)
                pq.poll();
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            /*
                key   : a random value between 0 and N (total number of points)
                value : the point
            */
            for (Entry e: pq){
                outputKey.set(e.getPriority());
                context.write(outputKey, e.getPoint());
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
        FileOutputFormat.setOutputPath(job, new Path(conf.get("startingMeans")));

        // Exit
        return job.waitForCompletion(true);
    }
}
