package nca;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.util.FastMath;
import org.apache.hadoop.contrib.utils.join.DataJoinMapperBase;
import org.apache.hadoop.contrib.utils.join.DataJoinReducerBase;
import org.apache.hadoop.contrib.utils.join.TaggedMapOutput;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NCA {
    // TODO function to load matrix A from file
    // TODO function to load label aka y from file
    public static class ExpSquaredNormMapper
            extends Mapper<Text, VectorWritable, Text, DoubleWritable> {

        private RealMatrix a;
        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            super.setup(context);
            // TODO load matrix A from hdfs, we need set path of this file in context
            MatrixUtils.deserializeRealMatrix();
        }

        @Override
        protected void map(Text key, VectorWritable value, Context context)
                throws IOException, InterruptedException {
            RealMatrix vector = Utils.parseVector(value.get());
            double result = FastMath.exp(FastMath.pow(a.multiply(vector).getFrobeniusNorm(),2));
            context.write(key, new DoubleWritable(result));
        }
    }

    public static class SumExpSquaredNormMapper
            extends Mapper<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void map(Text key, DoubleWritable value, Context context)
                throws IOException, InterruptedException {
            String[] keyPair = key.toString().split(",");
            if(!keyPair[0].equals(keyPair[1]))
                context.write(new Text(keyPair[0]), value);
        }
    }

    public static class SumExpSquaredNormReducer
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double result = 0.0;
            for(DoubleWritable value: values)
                result += value.get();
            context.write(key, new DoubleWritable(result));
        }
    }

    // TODO use DataJoin to compute p_{ij}
    public static class PMapper extends DataJoinMapperBase {

        @Override
        protected Text generateInputTag(String inputFile) {
            return new Text(inputFile);
        }

        @Override
        protected TaggedMapOutput generateTaggedMapOutput(Object value) {
            TaggedWritable retval = new TaggedWritable();
            retval.setTag(this.inputTag);
            retval.setData((Writable)value);
            return retval;
        }

        @Override
        protected Text generateGroupKey(TaggedMapOutput aRecord) {
            // TODO generateGroupKey according to their tag
            return null;
        }
    }

    public static class PReducer extends DataJoinReducerBase {

        @Override
        protected TaggedMapOutput combine(Object[] tags, Object[] values) {
            //
            return null;
        }
    }

}
