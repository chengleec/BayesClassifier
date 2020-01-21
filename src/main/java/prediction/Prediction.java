package prediction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Hashtable;

public class Prediction {

    //先验概率
    private static Hashtable<String,Double> class_prob = new Hashtable<>();
    //每个单词在每个类别中的概率
    private static Hashtable<String,Double> class_term_prob = new Hashtable<>();
    //每个类别中的单词总数
    private static Hashtable<String,Double> class_term_total = new Hashtable<>();

    public static void prediction() throws IOException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream in = null;
        BufferedReader reader = null;

        //统计文档总数
        in = fs.open(new Path("/bayes/training_output/doc_output/part-r-00000"));
        reader = new BufferedReader(new InputStreamReader(in));
        double fileTotal = 0;
        while(reader.ready()){
            String[] strings = reader.readLine().split("\\s+");
            //string[0]为类别名，string[1]为文档数量，每个类别文档数量相加得到文档总数
            fileTotal += Double.valueOf(strings[1]);
        }

        //计算先验概率
        in = fs.open(new Path("/bayes/training_output/doc_output/part-r-00000"));
        reader = new BufferedReader(new InputStreamReader(in));
        while (reader.ready()){
            String[] strings = reader.readLine().split("\\s+");
            //每个文档数量除以文档总数，得到先验概率
            class_prob.put(strings[0],Double.valueOf(strings[1])/fileTotal);
        }

        //计算单词总数
        in = fs.open(new Path("/bayes/training_output/wordsum_output/part-r-00000"));
        reader = new BufferedReader(new InputStreamReader(in));
        while(reader.ready()){
            String[] strings = reader.readLine().split("\\s+");
            //string[0]为类别名，string[1]为单词总数，将每个类别文档总数放入hashTable中
            class_term_total.put(strings[0],Double.valueOf(strings[1]));
        }

        //计算每个单词在相应类别中的概率
        in = fs.open(new Path("/bayes/training_output/word_output/part-r-00000"));
        reader = new BufferedReader(new InputStreamReader(in));
        while(reader.ready()){
            String[] strings = reader.readLine().split("\\s+");
            //string[0]为单词名，string[1]为类别名，string[2]为单词数量
            String key = strings[0] + "-" + strings[1];
            //将单词，类别，及单词在类别中的概率放入hashTable中
            class_term_prob.put(key,(Double.valueOf(strings[2]))/(class_term_total.get(strings[1])));
        }

        in.close();
        reader.close();
        fs.close();
    }

    //计算文档属于某个类别的概率
    public static double conditionalProbabilityForClass(String content,String className){
        double result = 0;
        String[] strings = content.split("\\s+");
        //将文档内容分割为一个个单词
        for (String string : strings) {
            String key = string + "-" + className;
            //将每个单词在该类别中的概率相乘，如果单词在该类别中不存在，则将其设为1/该类别中单词总数
            result += Math.log(class_term_prob.getOrDefault(key,1.0/(class_term_total.get(className))));
        }
        //最后乘以先验概率
        result += Math.log(class_prob.get(className));
        return result;
    }
}
