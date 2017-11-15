package triangle_count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class TriangleCount {

    public static class VertexPair implements WritableComparable<VertexPair> {
        // TODO change to string to fit google test bench
        private Long first;
        private Long second;

        public void set(long first, long second) {
            this.first = first;
            this.second = second;
        }

        public Long getFirst() {
            return first;
        }

        public Long getSecond() {
            return second;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeLong(first);
            dataOutput.writeLong(second);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            first = dataInput.readLong();
            second = dataInput.readLong();
        }

        @Override
        public int compareTo(VertexPair o) {
            if (first.equals(o.first))
                return second.compareTo(o.second);
            else
                return first.compareTo(o.first);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof VertexPair && compareTo((VertexPair) obj) == 0;
        }

        @Override
        public int hashCode() {
            return first.hashCode() ^ second.hashCode();
        }
    }

    public static class TriangleCountMapper
            extends Mapper<Text, Text, VertexPair, LongWritable> {

        VertexPair vertexPair = new VertexPair();

        // 输入为AdjList的输出
        @Override
        protected void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] valStrList = value.toString().split(" ");
            // convert key & value to int
            long keyVertex = Long.parseLong(key.toString());
            long[] vertexList = new long[valStrList.length];
            for (int i = 0; i < valStrList.length; ++i)
                vertexList[i] = Long.parseLong(valStrList[i]);
            // emit <<v1,v2>,[list of vn]>
            // v1 < v2 < any of vn
            Arrays.sort(vertexList);
            for (int i = 0; i < vertexList.length; ++i) {
                if (keyVertex < vertexList[i])
                    vertexPair.set(keyVertex, vertexList[i]);
                else
                    vertexPair.set(vertexList[i], keyVertex);
                for (int j = vertexList.length - 1; j > i && vertexList[j] > keyVertex; --j)
                    context.write(vertexPair, new LongWritable(vertexList[j]));
            }
        }
    }

    public static class TriangleCountReducer
            extends Reducer<VertexPair, LongWritable, NullWritable, LongWritable> {

        private Set<Long> vertexSet = new HashSet<>();
        private long count = 0;

        @Override
        protected void reduce(VertexPair key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            for (LongWritable vertex : values) {
                if (!vertexSet.contains(vertex.get()))
                    vertexSet.add(vertex.get());
                else
                    ++count;
            }
            vertexSet.clear();
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            context.write(NullWritable.get(), new LongWritable(count));
            super.cleanup(context);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        // change split size of input file to 1MB
        conf.setLong(FileInputFormat.SPLIT_MAXSIZE, 1024 * 1024);
        Job job = Job.getInstance(conf, "triangle count");
        job.setJarByClass(TriangleCount.class);
        job.setMapperClass(TriangleCountMapper.class);
        job.setReducerClass(TriangleCountReducer.class);
        job.setMapOutputKeyClass(VertexPair.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        KeyValueTextInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}
