package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class Clustering {
    public static class ClusteringMapper extends Mapper<LongWritable, Text, Point, AccumulatorPoint> {

        static int D;
        final static Map<Point, AccumulatorPoint> centroidSummation = new HashMap<>();

        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            D = Integer.parseInt(conf.get("d"));

            /*
                Prepare the hashmap used to build the in-mapper combiner.
                The hashmap will contain an entry for each mean
                and the tuple composed by the summation of the closest points
                and the number of already summed points:
                { mean: (sum, n) }
             */

            centroidSummation.clear();

            /* Get the means from cache, either sampled or computed in the previous step */
            URI[] cacheFiles = context.getCacheFiles();
            FileSystem fs = FileSystem.get(conf);

            for (URI f: cacheFiles) {
                InputStream is = fs.open(new Path(f));
                BufferedReader br = new BufferedReader(new InputStreamReader(is));

                String line;
                while ((line = br.readLine()) != null) {
                    Point mean = new Point(line);
                    centroidSummation.put(mean, new AccumulatorPoint(D));
                }

                br.close();
            }
        }

        public void map(LongWritable key, Text value, Context context) {
            /*
                The mapper gets a point and compute the distance between this point
                and all the means in the hashmap. Once we have the closest mean
                we can sum the point to the value associated to the mean in the hashmap
                updating the number of added points.
                Every mapper will build its own hashmap that will be merged in the reducer.
                The mapper will emit every mean associated with the the partial sum:
                {
                    key: mean
                    value: (partial sum, size)
                }
             */

            double minDistance = Double.POSITIVE_INFINITY;
            Point closestMean = null;

            Point p = new Point(value.toString());

            /*
                Compute the distance between point p and all the means
                in order to find the closest mean
             */

            for (Point m: centroidSummation.keySet()){
                double d = p.getSquaredDistance(m);
                if (d < minDistance){
                    minDistance = d;
                    closestMean = m;
                }
            }

            /*
                Once we got the closest mean, we update the value of
                the associated sum adding point p belonging to the cluster
             */

            AccumulatorPoint ap = centroidSummation.get(closestMean);
            ap.add(p);
            centroidSummation.put(closestMean, ap);
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Point, AccumulatorPoint> entry: centroidSummation.entrySet()){
                context.write(entry.getKey(), entry.getValue());
            }
        }
    }

    public static class ClusteringReducer extends Reducer<Point, AccumulatorPoint, NullWritable, Point> {

        static int D;

        public void setup(Context context){
            Configuration conf = context.getConfiguration();
            D = Integer.parseInt(conf.get("d"));
        }

        public void reduce(Point key, Iterable<AccumulatorPoint> values, Context context) throws IOException, InterruptedException {
            /*
                For each mean we sum all the partial summation got from different mappers,
                divide it by the number of summed points and emit the new centroid
             */

            Point centroid = new Point(D);
            int n = 0;

            for (AccumulatorPoint ap: values){
                centroid.add(ap);
                n += ap.getSize();
            }
            centroid.div(n);

            context.write(null, centroid);
        }
    }

    public static boolean main(Job job) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = job.getConfiguration();
        int K = Integer.parseInt(conf.get("k"));
        int numReduceTasks = Math.min(K, Integer.parseInt(conf.get("maxNumberOfReduceTasks")));

        job.setJarByClass(Clustering.class);

        job.setMapperClass(ClusteringMapper.class);
        job.setReducerClass(ClusteringReducer.class);

        // The best solution would be having one reducer per mean
        job.setNumReduceTasks(numReduceTasks);

        job.setMapOutputKeyClass(Point.class);
        job.setMapOutputValueClass(AccumulatorPoint.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Point.class);

        // Define input and output path file
        FileInputFormat.addInputPath(job, new Path(conf.get("input")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("finalMeans")));

        // Exit
        return job.waitForCompletion(true);
    }
}
