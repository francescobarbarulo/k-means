package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;


public class kMeans {

    public static class kMeansMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) {}
    }

    public static class kMeansReducer extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) {}
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 4){
            System.err.println("Usage: kMeans <d> <k> <in> <out>");
            System.exit(1);
        }

        System.out.println("d=" + otherArgs[0]);
        System.out.println("k=" + otherArgs[1]);
        System.out.println("input=" + otherArgs[2]);
        System.out.println("output=" + otherArgs[3]);

        conf.set("d", otherArgs[0]);
        conf.set("k", otherArgs[1]);

        Job job = Job.getInstance(conf, "k-means");

        // Set JAR class
        job.setJarByClass(kMeans.class);

        // Set Mapper class
        job.setMapperClass(kMeansMapper.class);

        // Set Reducer class
        job.setReducerClass(kMeansReducer.class);

        // Set key-value output format
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Define input and output path file
        FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

        // Exit
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
