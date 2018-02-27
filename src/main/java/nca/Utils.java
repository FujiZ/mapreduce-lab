package nca;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class Utils {
    static RealMatrix deserializeMatrix(DataInput dataInput) throws IOException {
        final int n = dataInput.readInt();
        final int m = dataInput.readInt();
        final double[][] data = new double[n][m];
        for (int i = 0; i < n; ++i) {
            final double[] dataI = data[i];
            for (int j = 0; j < m; ++j) {
                dataI[j] = dataInput.readDouble();
            }
        }
        return MatrixUtils.createRealMatrix(data);
    }

    static void serializeRealMatrix(RealMatrix matrix, DataOutput dataOutput) throws IOException {
        final int n = matrix.getRowDimension();
        final int m = matrix.getColumnDimension();
        dataOutput.writeInt(n);
        dataOutput.writeInt(m);
        for (int i = 0; i < n; ++i) {
            for (int j = 0; j < m; ++j) {
                dataOutput.writeDouble(matrix.getEntry(i, j));
            }
        }
    }

}
