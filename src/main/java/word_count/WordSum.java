package word_count;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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

import java.io.IOException;

public class WordSum {

    public static class WordSumMapper extends Mapper<Text, Text, Text, LongWritable> {
        @Override
        protected void map(Text key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            //分割value
            String[] strings = value.toString().split("\\s+");
            for (String string : strings) {
                FileSplit fileSplit = (FileSplit) context.getInputSplit();
                //获取类别名
                String dirName = fileSplit.getPath().getName();
                context.write(new Text(dirName),new LongWritable(1));
            }
        }
    }

    public static class WordSumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key,new LongWritable(sum));
        }
    }

    public static class WordSumRunner extends Configured implements Tool {
        public int run(String[] args) throws Exception {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);

            job.setJarByClass(WordSumRunner.class);

            job.setMapperClass(WordSumMapper.class);
            job.setReducerClass(WordSumReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            Path path = new Path(args[1]);
            MyUtils.deletePath(path);
            FileOutputFormat.setOutputPath(job,path);

            return job.waitForCompletion(true) ? 0 : 1;
        }
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(),new WordSumRunner(),args);
    }
}
