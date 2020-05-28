package it.unipi.hadoop;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

/* Multiple Dimension Point custom structure */
public class Point implements WritableComparable<Point> {
    private final ArrayPrimitiveWritable coordinates;

    public Point(){
        this.coordinates = new ArrayPrimitiveWritable();
    }

    public void set(final double[] coordinates){
        this.coordinates.set(coordinates);
    }

    public static Point zeroes(int d){
        double[] coordinates = new double[d];
        Arrays.fill(coordinates, 0.0);

        Point p = new Point();
        p.set(coordinates);
        return p;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        coordinates.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        coordinates.readFields(in);
    }

    public static Point parse(String value){
        String[] indicesAndValues = value.split(",");

        double[] coordinates = new double[indicesAndValues.length];
        for (int i = 0; i < coordinates.length; i++) {
            coordinates[i] = Double.parseDouble(indicesAndValues[i]);
        }

        Point p = new Point();
        p.set(coordinates);
        return p;
    }

    public double getDistance(Point that){
        double sum = 0;
        double[] thisCoordinates = (double[]) this.coordinates.get();
        double[] thatCoordinates = (double[]) that.coordinates.get();

        for (int i = 0; i < thisCoordinates.length; i++){
            sum += (thisCoordinates[i] - thatCoordinates[i])*(thisCoordinates[i] - thatCoordinates[i]);
        }

        return Math.sqrt(sum);
    }

    public void add(Point that){
        double[] thisCoordinates = (double[]) this.coordinates.get();
        double[] thatCoordinates = (double[]) that.coordinates.get();
        for (int i = 0; i < thisCoordinates.length; i++){
            thisCoordinates[i] += thatCoordinates[i];
        }
    }

    public void div(int n){
        double[] tmp = (double[]) this.coordinates.get();
        for (int i = 0; i < tmp.length; i++){
            tmp[i] /= n;
        }
    }

    public int hashCode(){
        return new Text(toString()).hashCode();
    }

    public String toString(){
        double[] tmpDouble = (double[]) this.coordinates.get();
        String[] tmpStr = new String[tmpDouble.length];
        for (int i = 0; i < tmpStr.length; i++)
            tmpStr[i] = Double.toString(tmpDouble[i]);
        return String.join(",", tmpStr);
    }

    @Override
    public int compareTo(Point that) {
        double[] thisCoordinates = (double[]) this.coordinates.get();
        double[] thatCoordinates = (double[]) that.coordinates.get();

        for (int i = 0; i < thisCoordinates.length; i++){
            if (thisCoordinates[i] < thatCoordinates[i])
                return -1;

            if (thisCoordinates[i] > thatCoordinates[i])
                return 1;
        }

        return 0;
    }
}
