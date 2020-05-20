
package it.unipi.hadoop;

import java.io.IOException;
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


public class Clustering_NewMeans {
        
    public static class Clustering_NewMeansMapper extends Mapper<LongWritable, Text, Point, PartialNewMean> {
        public static final Point meanPoint = new Point();
        public static final Point dataPoint = new Point();
        public static final PartialNewMean partialNewMean = new PartialNewMean();
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] meanAndDataString = value.toString().split("\t");     
            
            meanPoint.set(Point.parse(meanAndDataString[0]));
            dataPoint.set(Point.parse(meanAndDataString[1]));
            partialNewMean.set(dataPoint, 1);
            
            context.write(meanPoint, partialNewMean);
        }   
    }
    
    public static class Clustering_NewMeansCombiner extends Reducer<Point, PartialNewMean, Point, PartialNewMean> {
        private static final Point partialSum = new Point();
        private static final PartialNewMean partialNewMean = new PartialNewMean();

        public void reduce(Point key, Iterable<PartialNewMean> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            
            // Initial point with coordinates equal to 0.
            // Id is the same of the relative mean point.
            partialSum.set(new double[Integer.parseInt(conf.get("numberOfDimensions"))], PointType.MEAN, key.getId().get());
            long numberOfPoints = 0;
            
            for(PartialNewMean partialMean : values) {
                partialSum.add(partialMean.getPartialMean());
                numberOfPoints += partialMean.getNumberOfPoints().get();
            }
            
            partialNewMean.set(partialSum, numberOfPoints);
            context.write(key, partialNewMean);
        }
    }
    
    public static class Clustering_NewMeansReducer extends Reducer<Point, PartialNewMean, Point, Point> {
        private static final Point newMean = new Point();
        
        public void reduce(Point key, Iterable<PartialNewMean> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            
            // Initial point with coordinates equal to 0.
            // Id is the same of the relative mean point.
            newMean.set(new double[Integer.parseInt(conf.get("numberOfDimensions"))], PointType.MEAN, key.getId().get());
            long numberOfPoints = 0;
            
            for(PartialNewMean partialMean : values) {
                newMean.add(partialMean.getPartialMean());
                numberOfPoints += partialMean.getNumberOfPoints().get();
            }
            
            newMean.div(numberOfPoints);
            
            // Emit the old mean and its updated version.
            context.write(key, newMean);
        }
    }
    
    public static boolean main(Job job) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = job.getConfiguration();

        // Set JAR class.
        job.setJarByClass(Clustering_NewMeans.class);

        // Set Mapper class.
        job.setMapperClass(Clustering_NewMeansMapper.class);

        // Set Combiner class.
        job.setCombinerClass(Clustering_NewMeansCombiner.class);

        // Set Reducer class. There can be multiple reducers.
        job.setReducerClass(Clustering_NewMeansReducer.class);
        job.setNumReduceTasks(Integer.parseInt(conf.get("clusteringNumberOfReduceTasks")));
        
        // Set key-value output format.
        job.setMapOutputKeyClass(Point.class);
        job.setMapOutputValueClass(PartialNewMean.class);
        job.setOutputKeyClass(Point.class);
        job.setOutputValueClass(Point.class);
        
        // Define input and output path file. 
        FileInputFormat.addInputPath(job, new Path(conf.get("clusteringClosestPoints")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("clusteringNewMeans")));

        // Exit
        return job.waitForCompletion(true);
    } 
}
