
package it.unipi.hadoop;

import java.io.IOException;
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
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Clustering_NewMeans {
        
    public static class Clustering_NewMeansMapper extends Mapper<LongWritable, Text, Point, PartialNewMean> {
        private static final Point meanPoint = new Point();
        private static final Point dataPoint = new Point();
        private static final PartialNewMean partialNewMean = new PartialNewMean();
        
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
            partialSum.set(new double[conf.getInt("numberOfDimensions", 1)], PointType.MEAN, key.getId().get());
            long numberOfPoints = 0;
            
            for(PartialNewMean partialMean : values) {
                partialSum.add(partialMean.getPartialMean());
                numberOfPoints += partialMean.getNumberOfPoints().get();
            }
            
            partialNewMean.set(partialSum, numberOfPoints);
            context.write(key, partialNewMean);
        }
    }
    
    public static class Clustering_NewMeansReducer extends Reducer<Point, PartialNewMean, NullWritable, Point> {
        private static final Point newMean = new Point();
        private static final DoubleWritable distanceBetweenMeans = new DoubleWritable();
        private static MultipleOutputs multipleOutputs;
        
        public void setup(Context context) {
             multipleOutputs = new MultipleOutputs(context);
        }
        
        public void reduce(Point key, Iterable<PartialNewMean> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            
            // Initial point with coordinates equal to 0.
            // Id is the same of the relative mean point.         
            newMean.set(new double[conf.getInt("numberOfDimensions", 1)], PointType.MEAN, key.getId().get());
            long numberOfPoints = 0;
            
            for(PartialNewMean partialMean : values) {
                newMean.add(partialMean.getPartialMean());
                numberOfPoints += partialMean.getNumberOfPoints().get();
            }
            
            newMean.div(numberOfPoints);
            distanceBetweenMeans.set(key.getDistance(newMean));
                     
            // Emit the new mean and the distance between the old and new means, where 
            // the distance is used as stop condition of the algorithm.
            // The '/part' in the path specifies to create a folder with the preceding name to store the outputs,
            // instead of an output file with the preceding name.
            // Ex. "newMeans" --> outputPath/newMeans-r-000x
            // Ex. "newMeans/part" --> outputPath/newMeans/part-r-000x
            multipleOutputs.write("newMeans", null, newMean, conf.get("clusteringNewMeans_NewMeans") + "/part");
            multipleOutputs.write("distanceBetweenMeans", null, distanceBetweenMeans, conf.get("clusteringNewMeans_DistanceBetweenMeans") + "/part");
        }
        
        public void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
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
        job.setNumReduceTasks(conf.getInt("clusteringNumberOfReduceTasks", 1));
        
        // Set key-value output format.
        job.setMapOutputKeyClass(Point.class);
        job.setMapOutputValueClass(PartialNewMean.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Point.class);
        
        // Define input and output path file. 
        FileInputFormat.addInputPath(job, new Path(conf.get("clusteringClosestPoints")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("clusteringNewMeans")));
        
        MultipleOutputs.addNamedOutput(job, "newMeans", TextOutputFormat.class, NullWritable.class, Point.class);
        MultipleOutputs.addNamedOutput(job, "distanceBetweenMeans", TextOutputFormat.class, NullWritable.class, DoubleWritable.class);
        
        // Avoid empty files produced by the reducer due to MultipleOutputs.
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        
        // Exit.
        return job.waitForCompletion(true);
    } 
}
