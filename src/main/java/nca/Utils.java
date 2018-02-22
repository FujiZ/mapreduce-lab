package nca;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

class Utils {
    static RealMatrix parseVector(Writable[] vector) {
        double[] result = new double[vector.length];
        for(int i =0;i<vector.length;++i)
            result[i] = ((DoubleWritable)vector[i]).get();
        return MatrixUtils.createColumnRealMatrix(result);
    }
}
