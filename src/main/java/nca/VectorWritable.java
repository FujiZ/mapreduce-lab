package nca;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;

public class VectorWritable extends ArrayWritable{
    public VectorWritable() {
        super(DoubleWritable.class);
    }
}
