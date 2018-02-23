package nca;

import org.apache.hadoop.io.DoubleWritable;

public class TaggedDouble extends TaggedEntry{
    public TaggedDouble() {
        super(DoubleWritable.class);
    }
}
