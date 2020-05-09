package it.unipi.hadoop;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Point implements Writable {
    private ArrayPrimitiveWritable vector;

    public Point(){
        this.vector = new ArrayPrimitiveWritable();
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

    public String toString(){
        double[] tmpDouble = (double[]) this.vector.get();
        String[] tmpStr = new String[tmpDouble.length];
        for (int i = 0; i < tmpStr.length; i++)
            tmpStr[i] = Double.toString(tmpDouble[i]);
        return String.join(",", tmpStr);
    }
}
