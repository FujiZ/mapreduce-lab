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
        final int n = value.getRowDimension();
        final int m = value.getColumnDimension();
        dataOutput.writeInt(n);
        dataOutput.writeInt(m);
        for (int i = 0; i < n; ++i) {
            for (int j = 0; j < m; ++j) {
                dataOutput.writeDouble(value.getEntry(i, j));
            }
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        final int n = dataInput.readInt();
        final int m = dataInput.readInt();
        final double[][] data = new double[n][m];
        for (int i = 0; i < n; ++i) {
            final double[] dataI = data[i];
            for (int j = 0; j < m; ++j) {
                dataI[j] = dataInput.readDouble();
            }
        }
        value = MatrixUtils.createRealMatrix(data);
    }

    public void set(RealMatrix value) {
        this.value = value;
    }

    public RealMatrix get() {
        return this.value;
    }
}
