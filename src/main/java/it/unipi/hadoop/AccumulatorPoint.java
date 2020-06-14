package it.unipi.hadoop;

/*
    This is used in clustering for the partial sum of points
    with the number of accumulated points (size)
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class AccumulatorPoint extends Point {
    private int size;

    public AccumulatorPoint(){
        super();
    }

    public AccumulatorPoint(int d){
        super(d);
        size = 0;
    }

    public int getSize() {
        return size;
    }

    public void add(Point that){
        super.add(that);
        size++;
    }

    public String toString(){
        return super.toString() + " " + this.size;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(size);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        size = in.readInt();
    }
}
