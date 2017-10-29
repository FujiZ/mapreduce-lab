package inverted_index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class TfIdf {


    public static class TfIdfMapper
            extends Mapper<Text, Text, Text, IntWritable> {
        private Text word = new Text();
        private IntWritable count = new IntWritable();

        @Override
        protected void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            // word -> (avg_count, file: count; ...)
            String keyStr = key.toString();
            String valStr = value.toString();
            String[] entrys = valStr.substring(valStr.indexOf(',') + 2).split(";\\s");    // +2 to skip spaces
            for (String entry : entrys) {
                if (!entry.isEmpty()) {
                    commitEntry(keyStr, entry, context);
                }
            }
        }

        private void commitEntry(String key, String entry, Context context)
                throws IOException, InterruptedException {
            // entry is filename: count
            String[] entryPair = entry.split(":\\s");
            // get author name from filename
            String author = entryPair[0].replaceFirst("[0-9]+.*$", "");
            word.set(author + ", " + key);
            count.set(Integer.parseInt(entryPair[1]));
            // (author, key)->count
            context.write(word, count);
        }
    }

    public static class TfIdfReducer
            extends Reducer<Text, IntWritable, Text, DoubleWritable> {

        private Map<String, Integer> authorFileMap = new HashMap<>();   // author-> total file of author
        private DoubleWritable tfIdf = new DoubleWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSystem fs = FileSystem.get(context.getConfiguration());
            for(URI uri: context.getCacheFiles()){
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(uri))));
                String line;
                while ((line = reader.readLine())!= null){
                    String author = line.replaceFirst("[0-9]+.*$", "");
                    Integer count = authorFileMap.get(author);
                    if(count!=null)
                        authorFileMap.put(author,count+1);
                    else
                        authorFileMap.put(author, 1);
                }
                reader.close();
            }
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int tf = 0;
            int fileCount = 0;
            for (IntWritable val : values) {
                tf += val.get();
                ++fileCount;
            }
            String author = key.toString().split(",\\s")[0];
            double idf = Math.log(authorFileMap.get(author) / (fileCount + 1.0));
            tfIdf.set(tf * idf);
            context.write(key, tfIdf);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "tf-idf");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(TfIdfMapper.class);
        job.setReducerClass(TfIdfReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.addCacheFile(new Path(args[0]).toUri());    // file list
        KeyValueTextInputFormat.addInputPath(job, new Path(args[1]));   // inverted index file
        FileOutputFormat.setOutputPath(job, new Path(args[2])); // output
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

