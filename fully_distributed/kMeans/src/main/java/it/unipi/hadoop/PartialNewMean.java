
package it.unipi.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;


public class PartialNewMean implements Writable {
    private final Point partialMean;
    private final LongWritable numberOfPoints;
    
    public PartialNewMean() {
        this.partialMean = new Point();
        this.numberOfPoints = new LongWritable();
    }
    
    public PartialNewMean(Point p, long numberOfPoints) {
        this();
        this.set(p, numberOfPoints);
    }
    
    public void set(Point p, long numberOfPoints) {
        this.partialMean.set(p.copy());
        this.numberOfPoints.set(numberOfPoints);
    }
    
    public Point getPartialMean() {
        return partialMean;
    }
    
    public LongWritable getNumberOfPoints() {
        return numberOfPoints;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        partialMean.write(out);
        numberOfPoints.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        partialMean.readFields(in);
        numberOfPoints.readFields(in);
    }
}
