package inverted_index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class InvertedIndex {

    public static class InvertedIndexMapper
            extends Mapper<Object,Text,Text,IntWritable>{
        private static final IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName().split("\\.")[0];
            StringTokenizer tokenizer=new StringTokenizer(value.toString().toLowerCase());
            while (tokenizer.hasMoreTokens()){
                word.set(tokenizer.nextToken() + "#" + fileName);
                context.write(word,one);
            }
        }
    }

    public static class InvertedIndexCombiner
            extends Reducer<Text,IntWritable,Text,IntWritable>{

        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val: values)
                sum+=val.get();
            result.set(sum);
            context.write(key,result);
        }
    }

    public static class InvertedIndexPartitioner
            extends HashPartitioner<Text,IntWritable> {

        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            String term = key.toString().split("#")[0];
            return super.getPartition(new Text(term), value, numReduceTasks);
        }
    }

    public static class InvertedIndexReducer
            extends Reducer<Text,IntWritable,Text,Text>{

        private Text lastWord = new Text();
        private Text curWord = new Text();
        private List<String> postingList = new ArrayList<>();   // file: count;
        private long totalCount = 0;

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            String[] keyPair = key.toString().split("#");
            curWord.set(keyPair[0]);
            String fileName = keyPair[1];
            int sum = 0;
            for(IntWritable val:values)
                sum += val.get();
            if(!lastWord.equals(curWord) && !postingList.isEmpty())
                commitResult(context);
            postingList.add(fileName+": " +sum);
            totalCount += sum;
            lastWord.set(keyPair[0]);
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            if(!postingList.isEmpty())
                commitResult(context);
            super.cleanup(context);
        }

        private void commitResult(Context context)
                throws IOException, InterruptedException {
            StringBuilder builder = new StringBuilder();
            builder.append(totalCount/(double)postingList.size());
            builder.append(", ");
            for(String str: postingList){
                builder.append(str);
                builder.append("; ");
            }
            context.write(lastWord, new Text(builder.toString()));
            totalCount = 0;
            postingList.clear();
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "inverted index");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(InvertedIndexCombiner.class);
        job.setPartitionerClass(InvertedIndexPartitioner.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
