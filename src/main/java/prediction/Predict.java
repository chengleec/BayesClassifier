package prediction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import utils.MyUtils;
import utils.SmallFileToSequenceFile;

import java.io.IOException;


public class Predict {

    public static class PredictMapper extends Mapper<Text, Text, Text, Text> {

        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            super.setup(context);
            Prediction.prediction();
        }

        @Override
        protected void map(Text key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            //获取文档真实类别
            String trueClass = fileSplit.getPath().getName();

            String[] ClassName = {"cn","aus"};
            for (String className : ClassName) {
                //计算文档在每个类别中的概率
                String prob = Double.toString(Prediction.conditionalProbabilityForClass(value.toString(),className));
                //文件输出格式为：文档名 真实类别 类别 概率
                context.write(new Text(key + " " + trueClass),new Text(className + " " + prob));
            }
        }
    }

    public static class PredictReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer.Context context) throws IOException, InterruptedException {
            String className = "";
            double maxProb = Double.NEGATIVE_INFINITY;
            //遍历每个文档在每个类别中的概率，求出最大概率
            for (Text value : values) {
                String[] strings = value.toString().split(" ");
                //string[0]为类别名，string[1]为文档在该类别中的概率
                if(Double.valueOf(strings[1]) > maxProb){
                    className = strings[0];
                    maxProb = Double.valueOf(strings[1]);
                }
            }
            //文件输出格式为：文档名 真实类别 预测类别
            context.write(key,new Text(className));
        }
    }

    public static class PredictRunner extends Configured implements Tool {

        @Override
        public int run(String[] args) throws Exception {

            SmallFileToSequenceFile.smallFileToSequenceFile(args);
            Configuration conf = new Configuration();
            conf.setBoolean("fs.hdfs.impl.disable.cache", true);
            Job job = Job.getInstance(conf);

            job.setJarByClass(Predict.class);
            job.setMapperClass(PredictMapper.class);
            job.setReducerClass(PredictReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);

            FileInputFormat.addInputPath(job, new Path(args[1]));
            Path path = new Path(args[2]);
            MyUtils.deletePath(path);
            FileOutputFormat.setOutputPath(job,path);

            return job.waitForCompletion(true) ? 0 : 1;
        }
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(),new PredictRunner(),args);
    }
}
