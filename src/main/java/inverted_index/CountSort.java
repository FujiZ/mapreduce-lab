package inverted_index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CountSort {
    public static class CountSortMapper
            extends Mapper<Object,Text,DoubleWritable,Text> {

        private DoubleWritable count = new DoubleWritable();

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String str = value.toString();
            int start = str.indexOf('\t') + 1;
            double count = Double.parseDouble(str.substring(start, str.indexOf(',',start)));
            this.count.set(count);
            context.write(this.count,value);
        }
    }

    public static class CountSortReducer
            extends Reducer<DoubleWritable,Text,NullWritable,Text> {
        @Override
        protected void reduce(DoubleWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for(Text val:values)
                context.write(NullWritable.get(), val);
        }
    }
    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "count sort");
        job.setJarByClass(CountSort.class);
        job.setMapperClass(CountSortMapper.class);
        job.setPartitionerClass(TotalOrderPartitioner.class);
        job.setReducerClass(CountSortReducer.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
