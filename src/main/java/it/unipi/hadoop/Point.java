package it.unipi.hadoop;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/* Multiple Dimension Point custom structure */
public class Point implements WritableComparable<Point> {
    private final ArrayPrimitiveWritable vector;

    public Point(){
        this.vector = new ArrayPrimitiveWritable();
    }

    public Point(final int d){
        this();

        double[] vector = new double[d];
        Arrays.fill(vector, 0.0);
        this.vector.set(vector);
    }

    public Point(final double[] vector){
        this();
        this.set(vector);
    }

    public void set(final double[] vector){
        this.vector.set(vector);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        vector.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        vector.readFields(in);
    }

    public static Point parse(String value){
        String[] indicesAndValues = value.split(",");

        double[] coordinates = new double[indicesAndValues.length];
        for (int i = 0; i < coordinates.length; i++) {
            coordinates[i] = Double.parseDouble(indicesAndValues[i]);
        }

        return new Point(coordinates);
    }

    public double getDistance(Point that){
        double sum = 0;
        double[] thisVector = (double[]) this.vector.get();
        double[] thatVector = (double[]) that.vector.get();

        for (int i = 0; i < thisVector.length; i++){
            sum += (thisVector[i] - thatVector[i])*(thisVector[i] - thatVector[i]);
        }

        return Math.sqrt(sum);
    }

    public void add(Point that){
        double[] thisVector = (double[]) this.vector.get();
        double[] thatVector = (double[]) that.vector.get();
        for (int i = 0; i < thisVector.length; i++){
            thisVector[i] += thatVector[i];
        }
    }

    public void div(int n){
        double[] tmp = (double[]) this.vector.get();
        for (int i = 0; i < tmp.length; i++){
            tmp[i] /= n;
        }
    }

    public String toString(){
        double[] tmpDouble = (double[]) this.vector.get();
        String[] tmpStr = new String[tmpDouble.length];
        for (int i = 0; i < tmpStr.length; i++)
            tmpStr[i] = Double.toString(tmpDouble[i]);
        return String.join(",", tmpStr);
    }

    @Override
    public int compareTo(Point that) {
        double[] thisVector = (double[]) this.vector.get();
        double[] thatVector = (double[]) that.vector.get();

        for (int i = 0; i < thisVector.length; i++){
            if (thisVector[i] < thatVector[i])
                return -1;

            if (thisVector[i] > thatVector[i])
                return 1;
        }

        return 0;
    }
}
