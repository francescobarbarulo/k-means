
package it.unipi.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


class Clustering_ClosestPoints {
    
    public static class Clustering_ClosestPointsPartitioner extends Partitioner<IdTypePair, Point> {

        // Ensures that data with the same Id goes to the same reducer.
        public int getPartition(IdTypePair key, Point value, int numberOfPartitions) {
            return (key.getId().hashCode() & Integer.MAX_VALUE) % numberOfPartitions;
        }
        
    }
       
    public static class Clustering_ClosestPointsGroupingComparator extends WritableComparator {
        
        public Clustering_ClosestPointsGroupingComparator() {
            super(IdTypePair.class, true);
        }
        
        // Controls which keys are grouped together into a single reduce() call.
        // In this case, the grouping is done by Id.
        public int compare(WritableComparable wc1, WritableComparable wc2) {
            IdTypePair pair = (IdTypePair) wc1;
            IdTypePair pair2 = (IdTypePair) wc2;
            return pair.getId().compareTo(pair2.getId());
        }
        
    }
   
    public static class Clustering_ClosestPointsMapper extends Mapper<LongWritable, Text, IdTypePair, Point> {
        private static final IdTypePair outputKey = new IdTypePair();
        private static final Point outputValue = new Point();
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            long numberOfPoints = conf.getLong("numberOfPoints", 1);
            outputValue.set(Point.parse(value.toString()));
            
            if (outputValue.getNumberOfDimensions() != conf.getInt("numberOfDimensions", -1)) {
                System.err.println("The point " + outputValue.toString() + " does not match the configured number of dimensions. It has been excluded from the iteration.");
                System.err.println("Point dimensions: " + outputValue.getNumberOfDimensions() + "; configured dimensions: " + conf.getInt("numberOfDimensions", -1));
                return;
            }
            
            if (outputValue.isData()) {
                outputKey.set(outputValue.getId().get(), PointType.DATA);
                context.write(outputKey, outputValue);
            } else if (outputValue.isMean()) {
                // Id of the points must go from 1 to numberOfPoints.
                for (long i = 1; i <= numberOfPoints; i++) {
                    outputKey.set(i, PointType.MEAN);
                    context.write(outputKey, outputValue);
                }
            }
        }
    }
    
    public static class Clustering_ClosestPointsCombiner extends Reducer<IdTypePair, Point, IdTypePair, Point> {
        private static final Point dataPoint = new Point();
        private static final Point closestMean = new Point();
            
        public void reduce(IdTypePair key, Iterable<Point> values, Context context) throws IOException, InterruptedException {
            double minimumDistance = Double.POSITIVE_INFINITY;
            boolean meanParsed = false;

            // Since a secondary sort pattern is applied, the first Point in the
            // Iterable<> is a data point, while the others are mean points.
            // Check if a data point was parsed by the mapper.
            if (values.iterator().hasNext()) {
                dataPoint.set(values.iterator().next().copy());
                if (!dataPoint.isData()) {
                    // No data point was parsed by the mapper: simply propagate all the received points.
                    // "dataPoint" is a mean point, in this case.
                    context.write(key, dataPoint); 
                    
                    for (Point p : values)
                        context.write(key, p);
                    
                    System.out.println("Clustering_ClosestPointsCombiner: no data point parsed by the mapper");
                    return;
                }      
            }
            
            // Data point parsed by the mapper: then, check if mean points were parsed.
            for (Point p : values) {
                if (p.isData())
                    throw new IllegalArgumentException("Error: secondary sort pattern is not working.");
                
                double distance = (dataPoint.getDistance(p))*(dataPoint.getDistance(p));
                if (distance < minimumDistance) {
                    minimumDistance = distance;
                    closestMean.set(p.copy());
                    meanParsed = true;
                }
            }

            context.write(key, dataPoint);
            
            // If no mean points were parsed by the mapper, it only emits the previously found dataPoint.
            if (!meanParsed)
                System.out.println("Clustering_ClosestPointsCombiner: no mean points parsed by the mapper");
            else
                context.write(key, closestMean);
        }
    }
    
    public static class Clustering_ClosestPointsReducer extends Reducer<IdTypePair, Point, Point, Point> {
        private static final Point dataPoint = new Point();
        private static final Point closestMean = new Point();

        public void reduce(IdTypePair key, Iterable<Point> values, Context context) throws IOException, InterruptedException {
            double minimumDistance = Double.POSITIVE_INFINITY;
            
            // Since a secondary sort pattern is applied, the first Point in the
            // Iterable<> is a data point, while the others are mean points.
            if (values.iterator().hasNext()) {
                dataPoint.set(values.iterator().next().copy());
                if (dataPoint.isMean())
                    throw new IllegalArgumentException("Error: secondary sort pattern is not working.");
            }
            
            // From now on, all the points in values are mean points.
            for (Point p : values) {
                if (p.isData())
                    throw new IllegalArgumentException("Error: secondary sort pattern is not working.");

                double distance = dataPoint.getDistance(p)*dataPoint.getDistance(p);
                if (distance < minimumDistance) {
                    minimumDistance = distance;
                    closestMean.set(p.copy());
                }
            }
            
            context.write(closestMean, dataPoint);
        }
    }
    
    public static boolean main(Job job) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = job.getConfiguration();

        // Set JAR class.
        job.setJarByClass(Clustering_ClosestPoints.class);

        // Set Mapper class.
        job.setMapperClass(Clustering_ClosestPointsMapper.class);

        // Set Combiner class.
        job.setCombinerClass(Clustering_ClosestPointsCombiner.class);

        // Set Reducer class. There can be multiple reducers.
        job.setReducerClass(Clustering_ClosestPointsReducer.class);
        job.setNumReduceTasks(conf.getInt("clusteringNumberOfReduceTasks", 1));
        
        // Set partitioner and grouping classes.
        // Needed to implement secondary sort and obtain sorted values in 
        // the Iterable<> given as input to a reducer and a combiner.
        job.setPartitionerClass(Clustering_ClosestPointsPartitioner.class);
        job.setGroupingComparatorClass(Clustering_ClosestPointsGroupingComparator.class);
        job.setCombinerKeyGroupingComparatorClass(Clustering_ClosestPointsGroupingComparator.class);

        // Set key-value output format.
        job.setMapOutputKeyClass(IdTypePair.class);
        job.setMapOutputValueClass(Point.class);
        job.setOutputKeyClass(Point.class);
        job.setOutputValueClass(Point.class);
        
        // CombineFileInputFormat mixes splits from differen files, so that the use of the custom combiner is effective.
        job.setInputFormatClass(CombineTextInputFormat.class);
        
        // Define input and output path file. 
        CombineTextInputFormat.addInputPath(job, new Path(conf.get("inputPath")));
        CombineTextInputFormat.addInputPath(job, new Path(conf.get("intermediateMeans")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("clusteringClosestPoints")));

        // Exit.
        return job.waitForCompletion(conf.getBoolean("verbose", true));
    }  
}