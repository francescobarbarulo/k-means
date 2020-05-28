package it.unipi.hadoop;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
    This is used in clustering for accumulating Point values (value)
    with the number of accumulated points (size)
 */

public class AccumulatorPoint implements Writable {
    private int size;
    private Point value;

    public AccumulatorPoint(){
        this.value = new Point();
    }

    public int getSize() {
        return size;
    }

    public Point getValue() {
        return value;
    }

    public void setValue(Point value){
        this.value = value;
    }

    public void setSize(int size) {
        this.size = size;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.value.write(out);
        out.writeInt(this.size);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.value.readFields(in);
        this.size = in.readInt();
    }

    public String toString(){
        return this.value.toString() + " " + this.size;
    }
}
