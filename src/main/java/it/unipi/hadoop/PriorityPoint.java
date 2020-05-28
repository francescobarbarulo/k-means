package it.unipi.hadoop;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
    Point associated with a priority
 */

public class PriorityPoint implements WritableComparable<PriorityPoint> {
    private int priority;
    private Point point;

    public PriorityPoint(){
        this.point = new Point();
    }

    public int getPriority() {
        return priority;
    }

    public Point getPoint() {
        return point;
    }

    public void setPriority(int priority){
        this.priority = priority;
    }

    public void setPoint(Point point){
        this.point = point;
    }

    @Override
    public int compareTo(PriorityPoint that) {
        // TODO -- decide priority rule: the lower the value, the higher the priority or viceversa?
        return Integer.compare(that.priority, this.priority);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.point.write(out);
        out.writeInt(this.priority);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.point.readFields(in);
        this.priority = in.readInt();
    }

    public String toString(){
        return this.point.toString() + " " + this.priority;
    }
}
