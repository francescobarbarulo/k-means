package it.unipi.hadoop;

/*
    Entry is used in two cases:
    1. Saving points in the PriorityQueue in Sampling : value = priority
    2. Saving partial sum of points in Clustering     : value = number of values
 */

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Entry implements WritableComparable<Entry> {
    private int value;
    private Point point;

    public Entry(){
        this.point = new Point();
    }

    public Entry(int d){
        this.value = 0;
        this.point = new Point(d);
    }

    public Entry(int value, Point point){
        this.value = value;
        this.point = point;
    }

    public int getValue() {
        return value;
    }

    public Point getPoint() {
        return point;
    }

    public void setValue(int value){
        this.value = value;
    }

    public void setPoint(Point point){
        this.point = point;
    }

    @Override
    public int compareTo(Entry that) {
        return Integer.compare(that.value, this.value);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.point.write(out);
        out.writeInt(this.value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.point.readFields(in);
        this.value = in.readInt();
    }

    public String toString(){
        return this.point.toString() + " " + this.value;
    }
}
