package triangle_count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class AdjList {
    // 假设输入没有重复边

    public static class AdjListMapper
            extends Mapper<Object, Text, IntWritable, IntWritable> {

        private IntWritable[] vertexPair = new IntWritable[2];

        // TODO 需要处理自指的边
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] valStrPair = value.toString().split("\\s");
            assert valStrPair.length == 2;
            vertexPair[0].set(Integer.parseInt(valStrPair[0]));
            vertexPair[1].set(Integer.parseInt(valStrPair[1]));
            if (vertexPair[0].get() != vertexPair[1].get()) {
                context.write(vertexPair[0], vertexPair[1]);
                context.write(vertexPair[1], vertexPair[0]);
            }
        }
    }

    /**
     * 对于图中的所有有向边，都视为无向边，即拥有两个方向
     */
    public static class AdjListUndirectedReducer
            extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

        private Set<Integer> vertexSet = new HashSet<>();

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            for (IntWritable vertex : values)
                vertexSet.add(vertex.get());
            StringBuilder builder = new StringBuilder();
            for (Integer vertex : vertexSet) {
                builder.append(vertex);
                builder.append(' ');
            }
            if (builder.length() > 0) {
                builder.deleteCharAt(builder.length() - 1); // delete last ' '
                context.write(key, new Text(builder.toString()));
            }
            vertexSet.clear();
        }
    }

    /**
     * 当且仅当在两个方向均有边时才视为一条无向边
     */
    public static class AdjListDirectedReducer
            extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

        private Set<Integer> vertexSet = new HashSet<>();

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            StringBuilder builder = new StringBuilder();
            for (IntWritable vertex : values) {
                if (!vertexSet.contains(vertex.get()))
                    vertexSet.add(vertex.get());
                else {
                    builder.append(vertex);
                    builder.append(' ');
                }
            }
            if (builder.length() > 0) {
                builder.deleteCharAt(builder.length() - 1);
                context.write(key, new Text(builder.toString()));
            }
            vertexSet.clear();
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "adjacency list");
        job.setJarByClass(AdjList.class);
        job.setMapperClass(AdjListMapper.class);
        // TODO can be changed to DirectedReducer
        job.setReducerClass(AdjListUndirectedReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}
