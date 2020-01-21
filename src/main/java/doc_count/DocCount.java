package doc_count;

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

import static utils.SmallFileToSequenceFile.smallFileToSequenceFile;

public class DocCount {

    public static class DocCountMapper extends Mapper<Text, Text, Text, LongWritable> {

        protected void map(Text key, Text value, Mapper<Text, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            //获取sequenceFile文件名
            String dirName = fileSplit.getPath().getName();
            context.write(new Text(dirName), new LongWritable(1));
        }
    }

    public static class DocCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            long sum = 0L;
            //求出每个类别文档总和
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static class DocCountRunner extends Configured implements Tool {

        public int run(String[] args) throws Exception {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf);

            smallFileToSequenceFile(args);

            job.setJarByClass(DocCount.DocCountRunner.class);
            job.setMapperClass(DocCount.DocCountMapper.class);
            job.setReducerClass(DocCount.DocCountReducer.class);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);

            FileInputFormat.addInputPath(job, new Path(args[1]));
            Path path = new Path(args[2]);
            MyUtils.deletePath(path);
            FileOutputFormat.setOutputPath(job, path);
            return job.waitForCompletion(true) ? 0 : 1;
        }
    }
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new DocCount.DocCountRunner(), args);
    }
}
