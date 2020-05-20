package it.unipi.hadoop;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;


public class Point implements WritableComparable {
    private final ArrayPrimitiveWritable coordinates;
    private PointType type;
    private final LongWritable id;

    public Point() {
        coordinates = new ArrayPrimitiveWritable();
        type = PointType.DATA;
        id = new LongWritable();
    }
    
    public Point(final double[] coordinates, PointType type, long id){
        this();       
        this.set(coordinates, type, id);
    }

    public void set(final double[] coordinates, PointType type, long id) {
        this.coordinates.set(coordinates);
        this.type = type;
        
        if (this.type == PointType.DATA) {
            this.id.set(id);
        } else {
            this.id.set(-1);
        }
    }
    
    public void set(Point p) {
        this.set((double[]) p.getCoordinates().get(), p.getType(), p.getId().get());
    }
    
    public ArrayPrimitiveWritable getCoordinates() {
        return this.coordinates;
    }
    
    public LongWritable getId() {
        return this.id;
    }
    
    public PointType getType() {
        return this.type;
    }
    
    public double getDistance(Point that){
        double sum = 0;
        double[] thisVector = (double[]) this.coordinates.get();
        double[] thatVector = (double[]) that.getCoordinates().get();

        for (int i = 0; i < thisVector.length; i++){
            sum += (thisVector[i] - thatVector[i])*(thisVector[i] - thatVector[i]);
        }

        return Math.sqrt(sum);
    }

    public void add(Point that){
        double[] thisVector = (double[]) this.coordinates.get();
        double[] thatVector = (double[]) that.getCoordinates().get();
        for (int i = 0; i < thisVector.length; i++){
            thisVector[i] += thatVector[i];
        }
    }
 
    public void div(long n){
        double[] coordinatesDouble = (double[]) this.coordinates.get();
        
        for (int i = 0; i < coordinatesDouble.length; i++){
            coordinatesDouble[i] /= n;
        }
    }

    public boolean isMean() {
        return this.type == PointType.MEAN;
    }
    
    public boolean isData() {
        return this.type == PointType.DATA;
    }
    
    public void changeTypeToData() {
        this.type = PointType.DATA;
    }
    
    public void changeTypeToMean() {
        this.type = PointType.MEAN;
    }

    public Point copy() {
        return new Point((double[]) this.coordinates.get(), this.type, this.id.get());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        coordinates.write(out);
        WritableUtils.writeEnum(out, type);
        id.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        coordinates.readFields(in);
        type = WritableUtils.readEnum(in, PointType.class);
        id.readFields(in);
    }
    
    @Override
    public String toString(){
        double[] coordinatesDouble = (double[]) this.coordinates.get();
        String[] coordinatesString = new String[coordinatesDouble.length];
        
        for (int i = 0; i < coordinatesString.length; i++)
            coordinatesString[i] = Double.toString(coordinatesDouble[i]);
        
        if (this.type == PointType.DATA) {
            return this.type.toString() + "," + this.id.get() + "," + String.join(",", coordinatesString);
        }
        
        return this.type.toString() + "," + String.join(",", coordinatesString);
        
    }

    @Override
    public int compareTo(Object o) {
        Point thatPoint = (Point) o;
        int compareId = this.id.compareTo(thatPoint.getId());
        
        if (compareId == 0) {
            if (this.type == thatPoint.getType()) {
                double[] thisVector = (double[]) this.coordinates.get();
                double[] thatVector = (double[]) thatPoint.getCoordinates().get();

                for (int i = 0; i < thisVector.length; i++) {
                    if (thisVector[i] < thatVector[i]){
                        return -1;
                    }

                    if (thisVector[i] > thatVector[i]) {
                        return 1;
                    }
                }

                return 0;
                
            } else if (this.type == PointType.DATA) {
                return -1;
            } else if (this.type == PointType.MEAN) {
                return 1;
            }
        }
        return compareId;
    }
    
    @Override
    public int hashCode() {
        return new Text(this.toString()).hashCode();
    }
    
    public static Point parse(String value){
        String[] valueElements = value.split(",");
        
        PointType parsedType = PointType.valueOf(valueElements[0]);
        long parsedId;
        int coordinatesStartingIndex;
        
        if (parsedType == PointType.DATA) {
            parsedId = Long.valueOf(valueElements[1]);
            coordinatesStartingIndex = 2;
        } else {
            parsedId = -1;
            coordinatesStartingIndex = 1;
        }
        
        double[] parsedCoordinates = new double[valueElements.length - coordinatesStartingIndex];
        for (int i = coordinatesStartingIndex; i < valueElements.length; i++) {
            parsedCoordinates[i - coordinatesStartingIndex] = Double.parseDouble(valueElements[i]);
        }

        return new Point(parsedCoordinates, parsedType, parsedId);
    }
}