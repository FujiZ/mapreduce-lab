package nca;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.util.FastMath;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class NCA {

    public static class XijMapper
            extends Mapper<Text, Text, Text, MatrixWritable> {
        @Override
        protected void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] valStr = value.toString().split(" ");
            double[] data = new double[valStr.length];
            for (int i = 0; i < valStr.length; ++i)
                data[i] = Double.parseDouble(valStr[i]);
            RealMatrix vector = MatrixUtils.createColumnRealMatrix(data);
            context.write(key, new MatrixWritable(vector));
        }
    }

    public static class ExpSquaredNormMapper
            extends Mapper<Text, MatrixWritable, Text, MatrixWritable> {

        private RealMatrix a;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            super.setup(context);
            // load matrix A from hdfs, we need set path of this file in context
            Path path = new Path(context.getCacheFiles()[0].getPath());
            FileSystem fs = path.getFileSystem(context.getConfiguration());
            DataInputStream inputStream = new DataInputStream(fs.open(path));
            a = Utils.deserializeMatrix(inputStream);
            inputStream.close();
        }

        @Override
        protected void map(Text key, MatrixWritable value, Context context)
                throws IOException, InterruptedException {
            String[] keyPair = key.toString().split(",");
            double result = 0;
            if (!keyPair[0].equals(keyPair[1]))
                result = FastMath.exp(-FastMath.pow(a.multiply(value.get()).getFrobeniusNorm(), 2));
            context.write(key, new MatrixWritable(result));
        }
    }

    // TODO compute x_{ij}x_{ij}^T
    public static class XXtMapper
            extends Mapper<Text, MatrixWritable, Text, MatrixWritable> {
        @Override
        protected void map(Text key, MatrixWritable value, Context context)
                throws IOException, InterruptedException {
            RealMatrix result = value.get().multiply(value.get().transpose());
            context.write(key, new MatrixWritable(result));
        }
    }

    public static class GroupMapper
            extends Mapper<Text, MatrixWritable, Text, MatrixWritable> {
        @Override
        protected void map(Text key, MatrixWritable value, Context context)
                throws IOException, InterruptedException {
            String[] keyPair = key.toString().split(",");
            context.write(new Text(keyPair[0]), value);
        }
    }

    public static class ZipMapper
            extends Mapper<Text, MatrixWritable, NullWritable, MatrixWritable> {
        @Override
        protected void map(Text key, MatrixWritable value, Context context)
                throws IOException, InterruptedException {
            context.write(NullWritable.get(), value);
        }
    }

    public static class GradientReducer
            extends Reducer<NullWritable, MatrixWritable, NullWritable, MatrixWritable> {
        RealMatrix a = null;
        double lr = 0.0;

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            super.setup(context);
            lr = context.getConfiguration().getDouble(NCAConfig.LEARNING_RATE, 0.0);
            Configuration conf = context.getConfiguration();
            Path path = new Path(conf.get(NCAConfig.MAT_A));
            FileSystem fs = path.getFileSystem(conf);
            DataInputStream inputStream = new DataInputStream(fs.open(path));
            a = Utils.deserializeMatrix(inputStream);
            inputStream.close();
        }

        @Override
        protected void reduce(NullWritable key, Iterable<MatrixWritable> values, Context context)
                throws IOException, InterruptedException {
            RealMatrix result = values.iterator().next().get();
            for (MatrixWritable value : values)
                result.add(value.get());
            // update mat A
            result = a.multiply(result);
            context.write(NullWritable.get(), new MatrixWritable(result));
            a = a.add(result.scalarMultiply(lr));

            Configuration conf = context.getConfiguration();
            Path path = new Path(conf.get(NCAConfig.MAT_A));
            FileSystem fs = path.getFileSystem(conf);
            if (fs.exists(path))
                fs.delete(path, false);
            DataOutputStream outputStream = new DataOutputStream(fs.create(path));
            Utils.serializeRealMatrix(a, outputStream);
            outputStream.close();
        }
    }

    public static class SameLabelMapper
            extends Mapper<Text, MatrixWritable, Text, MatrixWritable> {
        private Map<String, String> labels = new HashMap<>();

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            super.setup(context);
            // load labels from cache
            Path path = new Path(context.getCacheFiles()[0].getPath());
            FileSystem fs = path.getFileSystem(context.getConfiguration());
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                String[] entry = line.split("\t");
                labels.put(entry[0], entry[1]);
            }
            reader.close();
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
            for (MatrixWritable value : values)
                result.add(value.get());
            context.write(key, new MatrixWritable(result));
        }
    }

}
