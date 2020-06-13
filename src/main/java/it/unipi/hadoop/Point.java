package it.unipi.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/* Multiple Dimension Point custom structure */

public class Point implements WritableComparable<Point> {
    private ArrayList<Double> coordinates;

    public Point(){
        this.coordinates = new ArrayList<>();
    }

    public void set(final double[] coordinates){
        for (Double c: coordinates)
            this.coordinates.add(c);
    }

    public ArrayList<Double> get(){
        return coordinates;
    }

    public static Point zeroes(int d){
        double[] coordinates = new double[d];
        Arrays.fill(coordinates, 0.0);

        Point p = new Point();
        p.set(coordinates);
        return p;
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
        ArrayList<Double> thisCoordinates = this.get();
        ArrayList<Double> thatCoordinates = that.get();

        for (int i = 0; i < thisCoordinates.size(); i++){
            sum += (thisCoordinates.get(i) - thatCoordinates.get(i))*(thisCoordinates.get(i) - thatCoordinates.get(i));
        }

        return Math.sqrt(sum);
    }

    public void add(Point that){
        ArrayList<Double> thisCoordinates = this.get();
        ArrayList<Double> thatCoordinates = that.get();

        for (int i = 0; i < thisCoordinates.size(); i++){
            thisCoordinates.set(i, thisCoordinates.get(i) + thatCoordinates.get(i));
        }
    }

    public void div(int n){
        for (int i = 0; i < coordinates.size(); i++){
            coordinates.set(i, coordinates.get(i)/n);
        }
    }

    public int hashCode(){
        return new Text(toString()).hashCode();
    }

    public String toString(){
        String[] tmp = new String[coordinates.size()];
        for (int i = 0; i < tmp.length; i++)
            tmp[i] = Double.toString(coordinates.get(i));
        return String.join(",", tmp);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(coordinates.size());
        for (Double c: coordinates)
            out.writeDouble(c);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();

        coordinates = new ArrayList<>();
        for (int i = 0; i < size; i++)
            coordinates.add(in.readDouble());
    }

    @Override
    public int compareTo(Point that) {
        ArrayList<Double> thisCoordinates = this.get();
        ArrayList<Double> thatCoordinates = that.get();

        for (int i = 0; i < thisCoordinates.size(); i++){
            if (thisCoordinates.get(i) < thatCoordinates.get(i))
                return -1;

            if (thisCoordinates.get(i) > thatCoordinates.get(i))
                return 1;
        }

        return 0;
    }
}
