package triangle_count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
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
        private int first;
        private int second;

        public void set(int first, int second) {
            this.first = first;
            this.second = second;
        }

        public int getFirst() {
            return first;
        }

        public int getSecond() {
            return second;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeInt(first);
            dataOutput.writeInt(second);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            first = dataInput.readInt();
            second = dataInput.readInt();
        }

        @Override
        public int compareTo(VertexPair o) {
            if (first == o.first)
                return second - o.second;
            else
                return first - o.first;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof VertexPair && compareTo((VertexPair) obj) == 0;
        }

        @Override
        public int hashCode() {
            return first ^ second;
        }
    }

    public static class TriangleCountMapper
            extends Mapper<Text, Text, VertexPair, IntWritable> {

        VertexPair vertexPair = new VertexPair();

        // 输入为AdjList的输出
        @Override
        protected void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] valStrList = value.toString().split("\\s");
            // convert key & value to int
            int keyVertex = Integer.parseInt(key.toString());
            int[] vertexList = new int[valStrList.length];
            for (int i = 0; i < valStrList.length; ++i)
                vertexList[i] = Integer.parseInt(valStrList[i]);
            // emit <<v1,v2>,[list of vn]>
            // v1 < v2 < any of vn
            Arrays.sort(vertexList);
            for (int i = 0; i < vertexList.length; ++i) {
                if (keyVertex < vertexList[i])
                    vertexPair.set(keyVertex, vertexList[i]);
                else
                    vertexPair.set(vertexList[i], keyVertex);
                for (int j = vertexList.length - 1; j > i && vertexList[j] > keyVertex; --j)
                    context.write(vertexPair, new IntWritable(vertexList[j]));
            }
        }
    }

    public static class TriangleCountReducer
            extends Reducer<VertexPair, IntWritable, NullWritable, LongWritable> {

        private Set<Integer> vertexSet = new HashSet<>();
        private long count = 0;

        @Override
        protected void reduce(VertexPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable vertex : values) {
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
        Job job = Job.getInstance(conf, "triangle count");
        job.setJarByClass(TriangleCount.class);
        job.setMapperClass(TriangleCountMapper.class);
        job.setReducerClass(TriangleCountReducer.class);
        job.setMapOutputKeyClass(VertexPair.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        KeyValueTextInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}
