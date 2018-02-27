package nca;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataOutputStream;
import java.io.IOException;

public class NCADriver {
    private static Job reduceMat(String jobName, Class<? extends Mapper> mapperClass,
                                 Class<? extends Reducer> reducerClass,
                                 String input, String output) throws IOException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(NCA.class);

        job.setMapperClass(mapperClass);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MatrixWritable.class);

        job.setReducerClass(reducerClass);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MatrixWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(output));

        return job;
    }

    private static void parseXij(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {
        // raw_x_ij, label,
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "x_ij");
        job.setJarByClass(NCA.class);

        job.setMapperClass(NCA.XijMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MatrixWritable.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }

    private static void initMatA(int dim, Path path) throws IOException {
        RealMatrix matrix = MatrixUtils.createRealIdentityMatrix(dim);

        Configuration conf = new Configuration();

        FileSystem fs = path.getFileSystem(conf);
        DataOutputStream outputStream = new DataOutputStream(fs.create(path));
        Utils.serializeRealMatrix(matrix, outputStream);
        outputStream.close();
        fs.close();
    }

    private static void expNorm(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "exp_norm");
            job.setJarByClass(NCA.class);

            job.setMapperClass(NCA.ExpSquaredNormMapper.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(MatrixWritable.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.addCacheFile(new Path(args[2]).toUri());
            job.waitForCompletion(true);

    }

    public static void sumExpNorm(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {
        Job job = reduceMat("sum_exp_norm",NCA.GroupMapper.class,
                NCA.SumMatReducer.class, args[0], args[1]);
        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(),
                new MatJoin.MatBinaryOp("p_ij", MatJoin.GroupMapper.class,
                        MatJoin.NumDivReducer.class), args);
        System.exit(res);
    }
}
