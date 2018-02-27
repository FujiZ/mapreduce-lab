package nca;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.util.FastMath;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class NCA {

    public static class XijMapper
            extends Mapper<Text, Text, Text,MatrixWritable> {
        @Override
        protected void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] valStr = value.toString().split(" ");
            double[] data = new double[valStr.length];
            for(int i =0;i<valStr.length;++i)
                data[i]= Double.parseDouble(valStr[i]);
            RealMatrix vector = MatrixUtils.createColumnRealMatrix(data);
            context.write(key, new MatrixWritable(vector));
        }
    }

    // TODO function to load matrix A from file
    // TODO function to load label aka y from file
    public static class ExpSquaredNormMapper
            extends Mapper<Text, MatrixWritable, Text, MatrixWritable> {

        private RealMatrix a;
        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            super.setup(context);
            // TODO load matrix A from hdfs, we need set path of this file in context
            // MatrixUtils.deserializeRealMatrix();
        }

        @Override
        protected void map(Text key, MatrixWritable value, Context context)
                throws IOException, InterruptedException {
            String[] keyPair = key.toString().split(",");
            double result = 0;
            if (!keyPair[0].equals(keyPair[1]))
                result = FastMath.exp(-FastMath.pow(a.multiply(value.get()).getFrobeniusNorm(),2));
            context.write(key, new MatrixWritable(result));
        }
    }

    // TODO compute x_{ij}x_{ij}^T
    public static class XXTMapper
            extends Mapper<Text, MatrixWritable, Text, MatrixWritable> {
        @Override
        protected void map(Text key, MatrixWritable value, Context context)
                throws IOException, InterruptedException {
            RealMatrix result = value.get().multiply(value.get().transpose());
            context.write(key, new MatrixWritable(result));
        }
    }

    public static class AntiReflexiveMapper
            extends Mapper<Text, MatrixWritable, Text, MatrixWritable> {
        @Override
        protected void map(Text key, MatrixWritable value, Context context)
                throws IOException, InterruptedException {
            // emit all entries except i,i
            String[] keyPair = key.toString().split(",");
            if(!keyPair[0].equals(keyPair[1]))
                context.write(new Text(keyPair[0]), value);
        }
    }

    public static class GroupMapper
            extends Mapper<Text, MatrixWritable, Text, MatrixWritable> {
        @Override
        protected void map(Text key, MatrixWritable value, Context context) throws IOException, InterruptedException {
            String[] keyPair = key.toString().split(",");
            context.write(new Text(keyPair[0]),value);
        }
    }

    public static class SameLabelMapper
            extends Mapper<Text, MatrixWritable, Text, MatrixWritable> {
        private Map<String,String> labels = new HashMap<>();
        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            super.setup(context);
            // TODO load labels from cache
        }

        @Override
        protected void map(Text key, MatrixWritable value, Context context)
                throws IOException, InterruptedException {
            String[] keyPair = key.toString().split(",");
            String[] labelPair = new String[2];
            labelPair[0] = labels.get(keyPair[0]);
            labelPair[1] = labels.get(keyPair[1]);
            if (labelPair[0].equals(labelPair[1]))
                context.write(new Text(keyPair[0]), value);
        }
    }

    public static class SumMatReducer
            extends Reducer<Text, MatrixWritable, Text, MatrixWritable> {
        @Override
        protected void reduce(Text key, Iterable<MatrixWritable> values, Context context)
                throws IOException, InterruptedException {
            RealMatrix result = values.iterator().next().get();
            for(MatrixWritable value: values)
                result.add(value.get());
            context.write(key, new MatrixWritable(result));
        }
    }

}
