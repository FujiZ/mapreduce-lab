package nca;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.util.FastMath;
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
            extends Mapper<Text, MatrixWritable, Text, MatrixWritable> {

        private RealMatrix a;
        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            super.setup(context);
            // TODO load matrix A from hdfs, we need set path of this file in context
            MatrixUtils.deserializeRealMatrix();
        }

        @Override
        protected void map(Text key, MatrixWritable value, Context context)
                throws IOException, InterruptedException {
            double result = FastMath.exp(-FastMath.pow(a.multiply(value.get()).getFrobeniusNorm(),2));
            context.write(key, new MatrixWritable(result));
        }
    }

    public static class SumExpSquaredNormMapper
            extends Mapper<Text, MatrixWritable, Text, DoubleWritable> {
        @Override
        protected void map(Text key, MatrixWritable value, Context context)
                throws IOException, InterruptedException {
            String[] keyPair = key.toString().split(",");
            if(!keyPair[0].equals(keyPair[1]))
                context.write(new Text(keyPair[0]), new DoubleWritable(value.get().getEntry(0,0)));
        }
    }

    public static class SumExpSquaredNormReducer
            extends Reducer<Text, DoubleWritable, Text, MatrixWritable> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double result = 0.0;
            for(DoubleWritable value: values)
                result += value.get();
            context.write(key, new MatrixWritable(result));
        }
    }

    // TODO use DataJoin to compute p_{ij}
    public static class PMapper extends EntryJoinMapperBase {

        @Override
        protected Text generateInputTag(String inputFile) {
            // handle filenames like part-0000,part-0001,etc.
            return new Text(inputFile.split("-")[0]);
        }

        @Override
        protected TaggedMapOutput generateTaggedMapOutput(Object value) {
            TaggedEntry retv = new TaggedDouble();
            retv.setTag(this.inputTag);
            retv.setData((Writable)value);
            return retv;
        }

        @Override
        protected Text generateGroupKey(TaggedMapOutput aRecord) {
            Text key = ((TaggedEntry)aRecord).getKey();
            return new Text(key.toString().split(",")[0]);
        }
    }

    public static class PReducer extends EntryJoinReducerBase {

        @Override
        protected TaggedMapOutput combine(Object[] tags, Object[] values) {
            // tags: Text
            // values: TaggedEntry
            if (tags.length < 2)
                return null;
            // values are sorted according to tags
            // TODO compute exp/sum_exp
            // 需要根据具体的元素顺序决定运算顺序
            // return 0 if i=j
            TaggedEntry entry = new TaggedMatrix();
            entry.setKey();
            entry.setData();
            return entry;
        }
    }

    // TODO compute x_{ij}x_{ij}^T
    public static class XXTMapper extends Mapper<Text, MatrixWritable, Text, MatrixWritable> {
        @Override
        protected void map(Text key, MatrixWritable value, Context context)
                throws IOException, InterruptedException {
            RealMatrix result = value.get().multiply(value.get().transpose());
            context.write(key, new MatrixWritable(result));
        }
    }

    public static class NumMultMatMapper extends EntryJoinMapperBase{

        @Override
        protected Text generateInputTag(String inputFile) {
            // handle filenames like part-0000,part-0001,etc.
            return new Text(inputFile.split("-")[0]);
        }

        @Override
        protected TaggedMapOutput generateTaggedMapOutput(Object value) {
            TaggedEntry retv = new TaggedDouble();
            retv.setTag(this.inputTag);
            retv.setData((Writable)value);
            return retv;
        }

        @Override
        protected Text generateGroupKey(TaggedMapOutput aRecord) {
            return null;
        }
    }

    public static class

    public static class SumMatReducer extends Reducer<Text, MatrixWritable, Text, MatrixWritable> {
        @Override
        protected void reduce(Text key, Iterable<MatrixWritable> values, Context context) throws IOException, InterruptedException {
            RealMatrix result = values.iterator().next().get().copy();
            for(MatrixWritable value: values) {
                result.add(value.get());
            }
            context.write(key, new MatrixWritable(result));
        }
    }

}
