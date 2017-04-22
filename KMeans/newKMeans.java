import java.io.IOException;
import java.util.*;
import java.io.BufferedWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

        
public class newKMeans {
 public static int CLnum;
 public static int DataDim;
 public static int Iter;
 public static String Out;  

 //protected  void setup(Context context)
 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    private Text closestCenText = new Text();
    private Text dataText = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //System.out.println("In function Map...");
        String[] line = value.toString().split(",");
        if (line.length > DataDim){

            double[] data = new double[DataDim];
            String dataStr = "";
            String closestCenStr = ""; 
            dataStr = line[0];
            data[0] = Double.parseDouble(line[0]);
            for (int i = 1 ; i < DataDim ; i++){
                data[i] = Double.parseDouble(line[i]);
                dataStr = dataStr + "," + line[i];
            }
            //System.out.println("data: " + dataStr);
            dataStr = dataStr + "," + line[DataDim];
            double[] closestCen = findClosest(data);
            closestCenStr = Double.toString(closestCen[0]);
            for (int i = 1 ; i < DataDim ; i++){
                closestCenStr = closestCenStr + "," + Double.toString(closestCen[i]);
            }
            //System.out.println(closestCenStr + "\t" + dataStr);

            dataText.set(dataStr);
            closestCenText.set(closestCenStr);
            context.write(closestCenText, dataText);
        }
    }
 } 
        
 public static class Reduce extends Reducer<Text, Text, Text, Text> {
    private Text newCenText = new Text();
    private Text dataCategoryText = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
        //System.out.println("In function Reduce...");
        Hashtable<String, Integer> ht = new Hashtable<String, Integer>();
        String newCenStr = "";
        String dataCategoryStr = "";
        int dataNum = 0;
        //double[][] data = new double[Iterators.size(values.iterator())][DataDim];
        double[] avg = new double[DataDim]; 
        for (int i = 0 ; i < DataDim ; i++){
            avg[i] = 0;
        }
        for (Text val : values) {
            dataNum ++;
            String[] line = val.toString().split(",");
            for (int i = 0 ; i < DataDim ; i++){
                avg[i] += Double.parseDouble(line[i]);
            }
            //dataCategoryStr = dataCategoryStr + "," + line[DataDim];
            Integer temp = ht.get(line[DataDim]);
            if(temp != null) ht.put(line[DataDim], temp+1);
            else ht.put(line[DataDim], 1);
        }
        dataCategoryStr = "," + ht.toString();
        newCenStr = Double.toString(avg[0]/dataNum);
        for (int i = 1 ; i < DataDim ; i++){
            avg[i] = avg[i]/dataNum;
            newCenStr = newCenStr + "," + Double.toString(avg[i]);
        }
        //System.out.println(newCenStr + "\t" + dataCategoryStr);
        newCenText.set(newCenStr);
        dataCategoryText.set(dataCategoryStr);
        context.write(newCenText, dataCategoryText);
    }
 }

 public static double[] findClosest(double[] data){
    //System.out.println("In function findClosest()...");
    double[][] centroids = readCentroids(Iter-1);
    double min = Double.MAX_VALUE;
    int minIndex = 0;
    for (int i = 0 ; i < CLnum ; i ++ ){
        double distant = dist(centroids[i],data);
        if (distant < min){
            min = distant;
            minIndex = i;  
        } 
    }
    String cenStr = Double.toString(centroids[minIndex][0]);
    for (int i = 1 ; i < DataDim ; i++){
        cenStr = cenStr + "," + Double.toString(centroids[minIndex][i]);
    }
    //System.out.println(cenStr + "closest distant: " + min);
    return centroids[minIndex];
 }

 // distance function
 public static double dist(double[] list1, double[] list2){
    //System.out.println("In function dist()...");
    double result = 0;
    for (int i = 0 ; i< list1.length ; i++){
        result += Math.pow(list1[i]-list2[i],2);
        //System.out.println("list1[" + i + "]:" + list1[i] + ", list2[" + i + "]:" + list2[i]);
        //System.out.println("POW:" + Math.pow(list1[i]-list2[i],2));
    }
    //System.out.println("distant: " + Math.sqrt(result));
    return Math.sqrt(result);
 }

 // function to read Data
 private static double[][] readData(String path){
    //System.out.println("In function readData()...");
    double[][] data = new double[152][DataDim];
    try{
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        InputStreamReader isr = new InputStreamReader(hdfs.open(new Path("/user/jack/" + path)));
        BufferedReader br=new BufferedReader(isr);
        String line = br.readLine();
        int c = 0;
        while(line != null)
        {
            if (line.isEmpty() || line.trim().equals("") || line.trim().equals("\n")) {
                break;
            }else{
                String[] val = line.split(",");
                for(int j = 0; j < DataDim; j++)
                {
                    data[c][j] = Double.parseDouble(val[j]);
                }   
                //System.out.println(line);
                line = br.readLine();
                c++;
            }
            
        }
        br.close();
    }catch(Exception e){
        System.out.println("read data file error");
        System.out.println(e.toString());
        return null;
    }
    return data;
 }

 // function to read centroids
 public static double[][] readCentroids(int iter){
    //System.out.println("In function readsCentroids()...");
    double[][] centroids = new double[CLnum][DataDim];
    try{
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        InputStreamReader isr = new InputStreamReader(hdfs.open(new Path("/user/jack/" + Out + "/centroids" + Integer.toString(iter) + "/part-r-00000")));
        BufferedReader br=new BufferedReader(isr);
        String line = br.readLine();
        int c = 0;
        while(line != null)
        {
            String[] val = line.split(",");
            for(int j = 0; j < DataDim; j++)
            {
                centroids[c][j] = Double.parseDouble(val[j]);
            }   
            //System.out.println(line);
            line = br.readLine();
            c++;
        }
        br.close();
    }catch(Exception e){
        System.out.println("read centroids file error");
        System.out.println(e.toString());
        return null;
    }
    return centroids;
 }
 // function to read centroids
 private static void initCentroids(double[][] data){
    System.out.println("In function initCentroids()...");

    Random random = new Random();
    
    for(int i=0; i < (data.length/2) ; i ++){
         int index = random.nextInt(data.length);
         double[] tmp = data[index];
         data[index] = data[i];
         data[i] = tmp;
    }
    double[][] randomCentroids = new double[CLnum][DataDim];
    String[] result = new String[CLnum];

    

    for(int i=0 ; i < CLnum ; i++){
        result[i] = Double.toString(data[i][0]);
        for(int j=1 ; j < DataDim ; j++){
            randomCentroids[i][j] = data[i][j];
            result[i] = result[i] + "," + Double.toString(data[i][j]);
        }

    }

    
    Configuration conf = new Configuration();
    String uriFile = "hdfs://localhost:9000/user/jack/"+ Out + "/centroids0/part-r-00000";
    try{
        FileSystem fs = FileSystem.get(URI.create(uriFile),conf);
        Path file = new Path(uriFile);
        
        FSDataOutputStream fsStream = fs.create(file);
        BufferedWriter out = new BufferedWriter(new OutputStreamWriter(fsStream,"UTF-8"));
        for(int i=0 ; i < CLnum ; i++){
            out.write(result[i] + "\n");
        }
        out.flush();
        out.close();
        fsStream.close();
    }catch(Exception e){
        System.out.println(e.toString());
    }

 }
        
 public static void main(String[] args) throws Exception {
    /*
    * args[0]: input path
    * args[1]: output path
    * args[2]: Cluster Number
    * args[3]: Data Dimension
    * args[4]: Convergence Size
    */

    Iter = 0;
    // Cluster Number
    Out = args[1];
    CLnum = Integer.parseInt(args[2]);
    // Data Dimension
    DataDim = Integer.parseInt(args[3]);
    boolean done = false;
    
    double totalDistant = 0;
    double[][] previousCen = new double[CLnum][DataDim];
    double[][] currentCen = new double[CLnum][DataDim];

    initCentroids(readData(args[0]));

    previousCen = readCentroids(Iter);
    // repeat kmean MapReduce until convergence
    do{
        Iter ++;
        Configuration conf = new Configuration();
            
            Job job = new Job(conf, "kmeans");
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
            
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setJarByClass(newKMeans.class);
            
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
            
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]+"/centroids"+ Iter));
            
        job.waitForCompletion(true);

        // calculate convergent or not
        
        currentCen = readCentroids(Iter);
        String temp = "";
        for(int i = 0 ; i < CLnum ; i++){
            //System.out.println(
            totalDistant += dist(previousCen[i],currentCen[i]);
        }
        totalDistant = totalDistant/CLnum;
        System.out.println("Average Distant is now: " + totalDistant);
        System.out.println("Args[4] is : " + Double.parseDouble(args[4]));
        System.out.println("convergence achived: " + (totalDistant < Double.parseDouble(args[4])));
        if (totalDistant < Double.parseDouble(args[4])){ done = true;}

        previousCen = currentCen;
        
    }while(done == false);
    System.out.println("Final result is at\n  hdfs: /user/jack/" + Out + "/centroids" + Integer.toString(Iter));
 }
        
}