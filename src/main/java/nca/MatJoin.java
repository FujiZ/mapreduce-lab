package nca;

import org.apache.commons.math3.linear.RealMatrix;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.contrib.utils.join.TaggedMapOutput;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;

public class MatJoin {
    public static class DefaultMapper extends EntryJoinMapperBase {

        @Override
        protected Text generateGroupKey(TaggedMapOutput aRecord) {
            return ((TaggedEntry) aRecord).getKey();
        }
    }

    public static class GroupMapper extends EntryJoinMapperBase {

        @Override
        protected Text generateGroupKey(TaggedMapOutput aRecord) {
            Text key = ((TaggedEntry) aRecord).getKey();
            return new Text(key.toString().split(",")[0]);
        }
    }

    public static class NumDivReducer extends EntryJoinReducerBase {

        @Override
        protected TaggedMapOutput combine(Object[] tags, Object[] values) {
            // tags: Text
            // values: TaggedMatrix
            if (tags.length < 2)
                return null;
            // values are sorted according to tags
            double left = ((MatrixWritable) ((TaggedMatrix) values[0]).getData()).get().getEntry(0, 0);
            double right = ((MatrixWritable) ((TaggedMatrix) values[1]).getData()).get().getEntry(0, 0);
            TaggedEntry entry = new TaggedMatrix();
            entry.setKey(((TaggedMatrix) values[0]).getKey());
            if (left == 0.0)
                entry.setData(new MatrixWritable(0));
            else
                entry.setData(new MatrixWritable(left / right));
            return entry;
        }
    }

    public static class NumMulMatReducer extends EntryJoinReducerBase {

        @Override
        protected TaggedMapOutput combine(Object[] tags, Object[] values) {
            if (tags.length < 2)
                return null;
            TaggedEntry entry = new TaggedMatrix();
            double left = ((MatrixWritable) ((TaggedMatrix) values[0]).getData()).get().getEntry(0, 0);
            RealMatrix right = ((MatrixWritable) ((TaggedMatrix) values[1]).getData()).get();
            entry.setKey(((TaggedMatrix) values[0]).getKey());
            entry.setData(new MatrixWritable(right.scalarMultiply(left)));
            return entry;
        }
    }

    public static class MatSubReducer extends EntryJoinReducerBase {

        @Override
        protected TaggedMapOutput combine(Object[] tags, Object[] values) {
            // tags: Text
            // values: TaggedMatrix
            if (tags.length < 2)
                return null;
            // values are sorted according to tags
            RealMatrix left = ((MatrixWritable) ((TaggedMatrix) values[0]).getData()).get();
            RealMatrix right = ((MatrixWritable) ((TaggedMatrix) values[1]).getData()).get();
            TaggedEntry entry = new TaggedMatrix();
            entry.setKey(((TaggedMatrix) values[0]).getKey());
            entry.setData(new MatrixWritable(left.subtract(right)));
            return entry;
        }
    }

    public static class MatBinaryOp extends Configured implements Tool {
        private String jobName;
        private Class<? extends EntryJoinMapperBase> mapperClass;
        private Class<? extends EntryJoinReducerBase> reducerClass;

        public MatBinaryOp(String jobName, Class<? extends EntryJoinMapperBase> mapperClass,
                           Class<? extends EntryJoinReducerBase> reducerClass) {
            this.jobName = jobName;
            this.mapperClass = mapperClass;
            this.reducerClass = reducerClass;
        }

        @Override
        public int run(String[] args) throws Exception {
            // leftFile, rightFile, output
            Configuration conf = getConf();
            conf.set(NCAConfig.LEFT_FILE, args[0]);
            conf.set(NCAConfig.RIGHT_FILE, args[1]);
            conf.setLong("datajoin.maxNumOfValuesPerGroup", Long.MAX_VALUE);

            JobConf job = new JobConf(conf, MatBinaryOp.class);
            job.setJobName(jobName);

            job.setMapperClass(mapperClass);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(TaggedMatrix.class);

            job.setReducerClass(reducerClass);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(MatrixWritable.class);

            job.setInputFormat(SequenceFileInputFormat.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileInputFormat.addInputPath(job, new Path(args[1]));
            job.setOutputFormat(SequenceFileOutputFormat.class);
            FileOutputFormat.setOutputPath(job, new Path(args[2]));

            JobClient.runJob(job).waitForCompletion();
            return 0;
        }
    }
}
