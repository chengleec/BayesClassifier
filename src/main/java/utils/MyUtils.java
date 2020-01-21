package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;

public class MyUtils {
    public static ArrayList<Path> getPath(String path) throws IOException {
        ArrayList<Path> paths = new ArrayList();
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path p = new Path(path);
        if(fs.isDirectory(p)){
            FileStatus[] fileStatuses = fs.listStatus(p);
            for (FileStatus fileStatus : fileStatuses) {
                if(fileStatus.isDirectory()){
                    paths.add(fileStatus.getPath());
                }
            }
        }
        return paths;
    }

    public static void deletePath(Path path) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(path)){
            fs.delete(path,true);
        }
    }
}
