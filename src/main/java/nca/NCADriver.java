package nca;

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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

    private static Job mapMat(String jobName, Class<? extends Mapper> mapperClass,
                              Path input, Path output) throws IOException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(NCA.class);

        job.setMapperClass(mapperClass);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MatrixWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, input);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, output);

        return job;
    }

    private static Job reduceMat(String jobName, Class<? extends Mapper> mapperClass,
                                 Class<? extends Reducer> reducerClass,
                                 Path input, Path output) throws IOException {
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
        FileInputFormat.addInputPath(job, input);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, output);

        return job;
    }

    private Path rawXijPath;
    private Path labelPath;
    private int dim;
    private Path workDir;
    private Path matAPath;
    private double lr;

    public NCADriver(Path rawXijPath, Path labelPath, int dim,
                     Path workDir, Path matAPath, double lr) {
        this.rawXijPath = rawXijPath;
        this.labelPath = labelPath;
        this.dim = dim;
        this.workDir = workDir;
        this.matAPath = matAPath;
        this.lr = lr;
    }

    public void init() throws IOException, ClassNotFoundException, InterruptedException {
        // clean workDir
        Configuration conf = new Configuration();
        FileSystem fs = workDir.getFileSystem(conf);
        if (fs.exists(workDir))
            fs.delete(workDir, true);
        fs.mkdirs(workDir);

        // init matA
        RealMatrix matrix = MatrixUtils.createRealIdentityMatrix(dim);
        fs = matAPath.getFileSystem(conf);
        fs.delete(matAPath, true);
        DataOutputStream outputStream = new DataOutputStream(fs.create(matAPath));
        Utils.serializeRealMatrix(matrix, outputStream);
        outputStream.close();

        // compute x_ij && x_xt
        Job job = parseXij(new Path(workDir, "x_ij"));
        job.waitForCompletion(false);
        job = mapMat("x_xt", NCA.XXtMapper.class,
                new Path(workDir, "x_ij"), new Path(workDir, "x_xt"));
        job.waitForCompletion(false);
    }

    public void clean() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = workDir.getFileSystem(conf);
        fs.delete(new Path(workDir, "exp_norm"), true);
        fs.delete(new Path(workDir, "sum_exp_norm"), true);
        fs.delete(new Path(workDir, "p_ij"), true);
        fs.delete(new Path(workDir, "p_i"), true);
        fs.delete(new Path(workDir, "p_x_xt"), true);
        fs.delete(new Path(workDir, "sum_p_x_xt"), true);
        fs.delete(new Path(workDir, "p_sum_p_x_xt"), true);
        fs.delete(new Path(workDir, "same_label_sum_p_x_xt"), true);
        fs.delete(new Path(workDir, "gradient"), true);
        fs.delete(new Path(workDir, "total_gradient"), true);
    }

    public void train() throws Exception {
        clean();

        String[] jobArgs = new String[3];
        // exp_norm
        Job job = mapMat("exp_norm", NCA.ExpSquaredNormMapper.class,
                new Path(workDir, "x_ij"), new Path(workDir, "exp_norm"));
        job.addCacheFile(matAPath.toUri());
        job.waitForCompletion(false);

        // sum_exp_norm
        job = reduceMat("sum_exp_norm", NCA.GroupMapper.class,
                NCA.SumMatReducer.class, new Path(workDir, "exp_norm"),
                new Path(workDir, "sum_exp_norm"));
        job.waitForCompletion(false);

        // p_ij
        jobArgs[0] = new Path(workDir, "exp_norm").toString();
        jobArgs[1] = new Path(workDir, "sum_exp_norm").toString();
        jobArgs[2] = new Path(workDir, "p_ij").toString();
        ToolRunner.run(new Configuration(),
                new MatJoin.MatBinaryOp("p_ij", MatJoin.GroupMapper.class,
                        MatJoin.NumDivReducer.class), jobArgs);

        // p_i
        job = reduceMat("p_i", NCA.SameLabelMapper.class,
                NCA.SumMatReducer.class, new Path(workDir, "p_ij"),
                new Path(workDir, "p_i"));
        job.addCacheFile(labelPath.toUri());
        job.waitForCompletion(false);

        // p_x_xt
        jobArgs[0] = new Path(workDir, "p_ij").toString();
        jobArgs[1] = new Path(workDir, "x_xt").toString();
        jobArgs[2] = new Path(workDir, "p_x_xt").toString();
        ToolRunner.run(new Configuration(),
                new MatJoin.MatBinaryOp("p_x_xt", MatJoin.DefaultMapper.class,
                        MatJoin.NumMulMatReducer.class), jobArgs);

        // sum_p_x_xt
        job = reduceMat("sum_p_x_xt", NCA.GroupMapper.class,
                NCA.SumMatReducer.class, new Path(workDir, "p_x_xt"),
                new Path(workDir, "sum_p_x_xt"));
        job.waitForCompletion(false);

        // p_sum_p_x_xt
        jobArgs[0] = new Path(workDir, "p_i").toString();
        jobArgs[1] = new Path(workDir, "sum_p_x_xt").toString();
        jobArgs[2] = new Path(workDir, "p_sum_p_x_xt").toString();
        ToolRunner.run(new Configuration(),
                new MatJoin.MatBinaryOp("p_sum_p_x_xt", MatJoin.DefaultMapper.class,
                        MatJoin.NumMulMatReducer.class), jobArgs);

        // same_label_sum_p_x_xt
        job = reduceMat("same_label_sum_p_x_xt", NCA.SameLabelMapper.class,
                NCA.SumMatReducer.class, new Path(workDir, "p_x_xt"),
                new Path(workDir, "same_label_sum_p_x_xt"));
        job.addCacheFile(labelPath.toUri());
        job.waitForCompletion(false);

        // gradient
        jobArgs[0] = new Path(workDir, "p_sum_p_x_xt").toString();
        jobArgs[1] = new Path(workDir, "same_label_sum_p_x_xt").toString();
        jobArgs[2] = new Path(workDir, "gradient").toString();
        ToolRunner.run(new Configuration(),
                new MatJoin.MatBinaryOp("gradient", MatJoin.DefaultMapper.class,
                        MatJoin.MatSubReducer.class), jobArgs);

        // update matA
        job = updateGradient(new Path(workDir, "gradient"),
                new Path(workDir, "total_gradient"));
        job.waitForCompletion(false);
    }

    public void train(int epoch) throws Exception {
        for (int i = 0; i < epoch; ++i) {
            train();
        }
    }

    private Job updateGradient(Path input, Path output) throws IOException {
        // use one MR to update matA
        Configuration conf = new Configuration();
        conf.setDouble(NCAConfig.LEARNING_RATE, lr);
        conf.set(NCAConfig.MAT_A, matAPath.toString());

        Job job = Job.getInstance(conf, "update_gradient");
        job.setJarByClass(NCA.class);

        job.setMapperClass(NCA.ZipMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(MatrixWritable.class);

        job.setNumReduceTasks(1);
        job.setReducerClass(NCA.GradientReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(MatrixWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(job, input);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, output);

        return job;
    }

    private Job parseXij(Path output) throws IOException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "x_ij");
        job.setJarByClass(NCA.class);

        job.setMapperClass(NCA.XijMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MatrixWritable.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, rawXijPath);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job, output);
        return job;
    }

    public static void main(String[] args) throws Exception {
        //@args raw_x_ij, label, dim, dir, matA, learning_rate, epoch
        Path rawXijPath = new Path(args[0]);
        Path labelPath = new Path(args[1]);
        int dim = Integer.parseInt(args[2]);
        Path workDir = new Path(args[3]);
        Path matAPath = new Path(args[4]);
        double lr = Double.parseDouble(args[5]);
        int epoch = Integer.parseInt(args[6]);

        NCADriver driver = new NCADriver(rawXijPath, labelPath, dim, workDir, matAPath, lr);
        driver.init();
        driver.train(epoch);
    }
}
