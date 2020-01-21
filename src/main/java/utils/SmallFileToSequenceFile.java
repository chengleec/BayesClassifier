package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;

public class SmallFileToSequenceFile {

    public static void smallFileToSequenceFile(String args[]) throws IOException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Text key = new Text();
        Text value = new Text();

        //获取输入路径
        ArrayList<Path> paths = MyUtils.getPath(args[0]);

        for (Path path : paths) {

            String dirName = path.getName();
            //设置输出路径
            SequenceFile.Writer.Option bigFile = SequenceFile.Writer.file(new Path(args[1] + "/" + dirName));
            //设置输出key格式
            SequenceFile.Writer.Option keyClass = SequenceFile.Writer.keyClass(Text.class);
            //设置输出value格式
            SequenceFile.Writer.Option valueClass = SequenceFile.Writer.valueClass(Text.class);
            //构造writer
            SequenceFile.Writer writer = SequenceFile.createWriter(conf, bigFile, keyClass, valueClass);

            FileStatus[] files = fs.listStatus(path);
            //读取文件
            for (FileStatus file : files) {

                FSDataInputStream in = null;
                byte[] buffer = null;
                //设置key
                key.set(file.getPath().getName());
                //打开文件输入流
                in = fs.open(file.getPath());
                //缓存区
                buffer = new byte[(int) file.getLen()];
                //流的拷贝
                IOUtils.readFully(in, buffer, 0, buffer.length);
                //将缓存区数据输出到value
                value.set(new Text(buffer));
                IOUtils.closeStream(in);
                writer.append(key, value);
            }
            IOUtils.closeStream(writer);
        }
    }
}