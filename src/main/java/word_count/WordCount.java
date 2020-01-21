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


public class WordCount {

    public static class WordCountMapper extends Mapper<Text, Text, Text, LongWritable> {

        protected void map(Text key, Text value, Mapper<Text, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            //分割value，使其成为一个个的单词
            String[] words = value.toString().split("\\s+");
            for (String word : words) {
                FileSplit inputSplit = (FileSplit) context.getInputSplit();
                //获取类别名
                String dirName = inputSplit.getPath().getName();
                context.write(new Text(word + " " + dirName),new LongWritable(1));
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            long sum = 0L;
            //求出每个类别中每个单词数量
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static class WordCountRunner extends Configured implements Tool {

        public int run(String[] args) throws Exception {

            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf);
            job.setJarByClass(WordCount.WordCountRunner.class);
            job.setMapperClass(WordCount.WordCountMapper.class);
            job.setReducerClass(WordCount.WordCountReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
            job.setInputFormatClass(SequenceFileInputFormat.class);


            FileInputFormat.addInputPath(job,new Path(args[0]));
            Path path = new Path(args[1]);
            MyUtils.deletePath(path);
            FileOutputFormat.setOutputPath(job, path);
            return job.waitForCompletion(true) ? 0 : 1;
        }
    }


    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new WordCount.WordCountRunner(), args);
    }
}
