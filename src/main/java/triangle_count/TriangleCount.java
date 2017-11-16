package triangle_count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class TriangleCount {

    public static class TriangleCountMapper
            extends Mapper<Text, Text, Text, Text> {

        Text vertexPair = new Text();

        @Override
        protected void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            String keyVertex = key.toString();
            String[] vertexList = value.toString().split(" ");
            // emit <<v1,v2>,[list of vn]>
            // v1 < v2 < any of vn
            Arrays.sort(vertexList);
            int pivot = ~Arrays.binarySearch(vertexList, keyVertex);
            assert pivot >= 0;
            for (int i = 0; i < vertexList.length; ++i) {
                if (keyVertex.compareTo(vertexList[i]) < 0)
                    vertexPair.set(keyVertex + "#" + vertexList[i]);
                else
                    vertexPair.set(vertexList[i] + "#" + keyVertex);
                int k = Math.max(pivot, i + 1);
                for (int j = vertexList.length - 1; j >= k; --j)
                    context.write(vertexPair, new Text(vertexList[j]));
            }
        }
    }

    public static class TriangleCountReducer
            extends Reducer<Text, Text, NullWritable, LongWritable> {

        private Set<String> vertexSet = new HashSet<>();
        private long count = 0;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text vertex : values) {
                if (!vertexSet.contains(vertex.toString()))
                    vertexSet.add(vertex.toString());
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
        conf.setLong(FileInputFormat.SPLIT_MAXSIZE, 5 * 1024 * 1024);
        Job job = Job.getInstance(conf, "triangle count");
        job.setJarByClass(TriangleCount.class);
        job.setMapperClass(TriangleCountMapper.class);
        job.setReducerClass(TriangleCountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        KeyValueTextInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setNumReduceTasks(Integer.parseInt(args[2])); // set reducer num to 10
        job.waitForCompletion(true);
    }
}
