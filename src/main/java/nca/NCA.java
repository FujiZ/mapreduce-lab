package nca;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.util.FastMath;
import org.apache.hadoop.contrib.utils.join.TaggedMapOutput;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
            String[] keyPair = key.toString().split(",");
            double result = 0;
            if (!keyPair[0].equals(keyPair[1]))
                result = FastMath.exp(-FastMath.pow(a.multiply(value.get()).getFrobeniusNorm(),2));
            context.write(key, new MatrixWritable(result));
        }
    }

    // TODO use DataJoin to compute p_{ij}
    public static class PMapper extends EntryJoinMapperBase {

        @Override
        protected Text generateInputTag(String inputFile) {
            // handle filenames like part-0000,part-0001,etc.
            // determine this should be left or right.
            // TODO give tag like left/right
            return new Text(inputFile.split("-")[0]);
        }

        @Override
        protected TaggedMapOutput generateTaggedMapOutput(Object value) {
            TaggedEntry retv = new TaggedMatrix();
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

    public static class NumDivReducer extends EntryJoinReducerBase {

        @Override
        protected TaggedMapOutput combine(Object[] tags, Object[] values) {
            // tags: Text
            // values: TaggedDouble
            if (tags.length < 2)
                return null;
            // values are sorted according to tags
            TaggedEntry entry = new TaggedMatrix();
            double left = ((MatrixWritable)((TaggedMatrix)values[0]).getData()).get().getEntry(0,0);
            double right = ((MatrixWritable)((TaggedMatrix)values[1]).getData()).get().getEntry(0,0);
            entry.setKey(((TaggedMatrix)values[0]).getKey());
            if (left == 0.0)
                entry.setData(new MatrixWritable(0));
            else
                entry.setData(new MatrixWritable(left/right));
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

    public static class NumMulMatMapper extends EntryJoinMapperBase{

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
            return new Tex
            return null;
        }
    }

    public static class NumMulMatReducer extends EntryJoinReducerBase {

        @Override
        protected TaggedMapOutput combine(Object[] tags, Object[] values) {
            if (tags.length < 2)
                return null;
            TaggedEntry entry = new TaggedMatrix();
            double left = ((MatrixWritable)((TaggedMatrix)values[0]).getData()).get().getEntry(0,0);
            RealMatrix right = ((MatrixWritable)((TaggedMatrix)values[1]).getData()).get();
            entry.setKey(((TaggedMatrix)values[0]).getKey());
            entry.setData(new MatrixWritable(right.scalarMultiply(left)));
            return entry;
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

    public static class SumMatrixReducer
            extends Reducer<Text, MatrixWritable, Text, MatrixWritable> {
        @Override
        protected void reduce(Text key, Iterable<MatrixWritable> values, Context context)
                throws IOException, InterruptedException {
            RealMatrix result = values.iterator().next().get();
            for(MatrixWritable value: values) {
                result.add(value.get());
            }
            context.write(key, new MatrixWritable(result));
        }
    }

}
