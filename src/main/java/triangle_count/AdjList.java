package triangle_count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class AdjList {
    // 假设输入没有重复边
    public static class AdjListMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text[] vertexPair = new Text[2];

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            vertexPair[0] = new Text();
            vertexPair[1] = new Text();
            super.setup(context);
        }

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] valStrPair = value.toString().split(" ");
            vertexPair[0].set(valStrPair[0]);
            vertexPair[1].set(valStrPair[1]);
            if (!vertexPair[0].equals(vertexPair[1])) {
                context.write(vertexPair[0], vertexPair[1]);
                context.write(vertexPair[1], vertexPair[0]);
            }
        }
    }

    /**
     * 对于图中的所有有向边，都视为无向边，即拥有两个方向
     */
    public static class AdjListUndirectedReducer
            extends Reducer<Text, Text, Text, Text> {

        private Set<String> vertexSet = new HashSet<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text vertex : values)
                vertexSet.add(vertex.toString());
            StringBuilder builder = new StringBuilder();
            for (String vertex : vertexSet) {
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
        extends Reducer<Text, Text, Text, Text> {

    private Set<String> vertexSet = new HashSet<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        StringBuilder builder = new StringBuilder();
        for (Text vertex : values) {
            if (!vertexSet.contains(vertex.toString()))
                vertexSet.add(vertex.toString());
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
        switch (args[2]){
            case "directed":
                job.setReducerClass(AdjListDirectedReducer.class);
                break;
            case "undirected":
                job.setReducerClass(AdjListUndirectedReducer.class);
                break;
            default:
                throw new IllegalArgumentException(args[2]);
        }
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        if (!job.waitForCompletion(true))
            throw new RuntimeException("AdjList failed");
    }
}
