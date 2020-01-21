package prediction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Enumeration;
import java.util.Hashtable;


public class Evaluation {

    private static String[] ClassNames = {"cn","aus"};
    private static Hashtable<String,Integer> TP = new Hashtable<>();
    private static Hashtable<String,Integer> TN = new Hashtable<>();
    private static Hashtable<String,Integer> FP = new Hashtable<>();
    private static Hashtable<String,Integer> FN = new Hashtable<>();
    private static Hashtable<String,Double> Precision = new Hashtable<>();
    private static Hashtable<String,Double> Recall = new Hashtable<>();

    public static void predictionCalculate() throws IOException {


        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        //打开任务五输出的文件
        FSDataInputStream in = fs.open(new Path("/bayes/test_output/part-r-00000"));
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));

        while (reader.ready()){
            String[] values = reader.readLine().split("\\s+");
            //values[0]为文档名，values[1]为文档真实类别，values[2]为文档预测类别
            String truth = values[1];
            String prediction = values[2];
            for (String className : ClassNames) {
                //如果真实类别等于className且等于预测类别，TP<className，value+1>
                if(truth.equals(className) && prediction.equals(className)) TP.put(className, TP.getOrDefault(className,0)+1);
                //如果真实类别等于className且不等于预测类别，TN<className,value+1>
                else if(truth.equals(className) && !prediction.equals(className)) FN.put(className,TN.getOrDefault(className,0)+1);
                //如果真实类别不等于className但等于预测类别，FP<className，value+1>
                else if(!truth.equals(className) && prediction.equals(className)) FP.put(className,FP.getOrDefault(className,0)+1);
                //如果真实类别不等于className且不等于预测类别，FN<className,value+1>
                else if(!truth.equals(className) && !prediction.equals(className))TN.put(className,TN.getOrDefault(className,0)+1);
            }
        }
    }

    public static void macroAveraging(){

        for (String className : ClassNames) {
            //求出每个类别中的TP,FP,TN,FN的值，并计算precision和recall
            double tp = TP.getOrDefault(className,0);
            double fp = FP.getOrDefault(className,0);
            double tn = TN.getOrDefault(className,0);
            double fn = FN.getOrDefault(className,0);
            double precisionValue = tp/(tp+fn);
            double recallValue = tp/(tp+fp);
            Precision.put(className,precisionValue);
            Recall.put(className,recallValue);
            System.out.println(className + ":");
            System.out.println(tp + " " + fp + " " + tn + " " + fn);
        }

        double precision = 0, recall = 0;
        Enumeration<Double> precisionElements = Precision.elements();
        //求出所有类别的precision和
        while(precisionElements.hasMoreElements()){
            precision += precisionElements.nextElement();
        }

        Enumeration<Double> recallElements = Recall.elements();
        //求出所有类别的recall和
        while ((recallElements.hasMoreElements())){
            recall += recallElements.nextElement();
        }
        //计算宏平均
        System.out.println("Macro-Average:");
        System.out.println("precision : " + precision/Precision.size());
        System.out.println("recall : " + recall/Recall.size());
        System.out.println("F1 : " + (2*precision*recall)/(precision+recall));
    }
    public static void microAveraging(){

        double tp = 0, fp = 0, tn = 0, fn = 0, precision = 0, recall = 0;

        Enumeration<Integer> tpElements = TP.elements();
        while(tpElements.hasMoreElements()) tp += tpElements.nextElement();
        Enumeration<Integer> fpElements = FP.elements();
        while(fpElements.hasMoreElements()) fp += fpElements.nextElement();
        Enumeration<Integer> tnElements = TN.elements();
        while(tnElements.hasMoreElements()) tn += tnElements.nextElement();
        Enumeration<Integer> fnElements = FN.elements();
        while(fnElements.hasMoreElements()) fn += fnElements.nextElement();

        //计算微平均
        System.out.println("Micro-Average:");
        System.out.println("precision : " + tp/(tp+fn));
        System.out.println("recall : " + tp/(tp+fp));
        System.out.println("F1 : " + (2*precision*recall)/(precision+recall));
    }

    public static void main(String[] args) throws IOException {

        predictionCalculate();
        macroAveraging();
        microAveraging();
    }
}
