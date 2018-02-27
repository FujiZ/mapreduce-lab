package nca;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MatrixWritable implements Writable {
    private RealMatrix value = null;

    public MatrixWritable() {
    }

    public MatrixWritable(RealMatrix value) {
        this.value = value;
    }

    public MatrixWritable(double value) {
        this.value = MatrixUtils.createRealMatrix(1,1);
        this.value.setEntry(0,0,value);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        Utils.serializeRealMatrix(this.value, dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        value = Utils.deserializeMatrix(dataInput);
    }

    public void set(RealMatrix value) {
        this.value = value;
    }

    public RealMatrix get() {
        return this.value;
    }
}
