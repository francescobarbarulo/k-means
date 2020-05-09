package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Random;


public class MeansElection {

    public static class MeansElectionMapper extends Mapper<LongWritable, Text, IntWritable, Point> {

        final static Random rand = new Random(42);
        final static IntWritable outputKey = new IntWritable();
        final static Point outputValue = new Point();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int N = Integer.parseInt(conf.get("n"));

            String line = value.toString();
            String[] indicesAndValues = line.split(",");

            double[] coordinates = new double[indicesAndValues.length];
            for (int i = 0; i < coordinates.length; i++) {
                coordinates[i] = Double.parseDouble(indicesAndValues[i]);
            }

            outputKey.set(rand.nextInt(N));
            outputValue.set(coordinates);
            context.write(outputKey, outputValue);
        }
    }

    public static class MeansElectionReducer extends Reducer<IntWritable, Point, IntWritable, Point>{

        static int meansCount;

        public void setup(Context context){
            meansCount = 0;
        }

        public void reduce(IntWritable key, Iterable<Point> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int K = Integer.parseInt(conf.get("k"));

            for (Point p: values){
                if (meansCount < K){
                    context.write(key, p);
                    meansCount++;
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 4){
            System.err.println("Usage: hadoop jar target/kMeans-1.0-SNAPSHOT.jar it.unipi.hadoop.MeansElection <n> <k> <in> <out>");
            System.exit(1);
        }

        System.out.println("n=" + otherArgs[0]);
        System.out.println("k=" + otherArgs[1]);
        System.out.println("input=" + otherArgs[2]);
        System.out.println("output=" + otherArgs[3]);

        conf.set("n", otherArgs[0]);
        conf.set("k", otherArgs[1]);

        Job job = Job.getInstance(conf, "k-means");

        // Set JAR class
        job.setJarByClass(MeansElection.class);

        // Set Mapper class
        job.setMapperClass(MeansElectionMapper.class);

        // Set Combiner class
        job.setCombinerClass(MeansElectionReducer.class);

        // Set Reducer class
        job.setReducerClass(MeansElectionReducer.class);

        // Set key-value output format
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Point.class);

        // Define input and output path file
        FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

        // Exit
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
