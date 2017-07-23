package SecondrySort_examples;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.sun.xml.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;


//二次排序
public class SecondrySort {
	private static int flag=0;//输入文件中的总行数，也是map处理的文件行数
	private static int count=0;//记录处理次数
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		conf.set("mapred.jpb.tracker", "localhost:9001");
		String arg[]=new GenericOptionsParser(conf, new String[]{"/secondrysort_in","/secondrysort_out"}).getRemainingArgs();
		if(arg.length!=2){
			System.err.println("in out file error!");
			System.exit(1);
		}
		FileSystem fs=FileSystem.get(conf);
		fs.delete(new Path(arg[1]), true);
		Job job=new Job(conf,"SecondrySort_job");
		job.setMapperClass(SecondrySortMapper.class);
		job.setReducerClass(SecondrySortReducer.class);
		job.setJarByClass(SecondrySort.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(arg[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
	public static class SecondrySortMapper extends Mapper<Object, Text, IntWritable, IntWritable>{
		static{//读取文件中有几行
			String filePath="hdfs://localhost:9000/secondrysort_in/secondrysort.txt";
			try {
				FileSystem fs=FileSystem.get(URI.create("hdfs://localhost:9000"),new Configuration());
				FSDataInputStream read = fs.open(new Path(filePath));
				InputStreamReader in=new InputStreamReader(read);//转换成字符流
				BufferedReader br=new BufferedReader(in);//包装
				while(br.readLine()!=null){
					flag++;
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		private static IntWritable outKey=new IntWritable();
		private static IntWritable outValue=new IntWritable();
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
					String val=value.toString();
					StringTokenizer st=new StringTokenizer(val);
					
					while(st.hasMoreTokens()){
						int first=Integer.parseInt(st.nextToken());
						int second=Integer.parseInt(st.nextToken());
						outKey.set(first);
						outValue.set(first+second);
					}
					context.write(outKey,outValue);//输出为<first,first+second>
					System.out.println("map 输出："+outKey+"\t"+outValue);
		}
	}	
	public static class SecondrySortReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
		private static Map<Integer,String> map=new LinkedHashMap<Integer,String>();//存放第一个数+第二个数 不能用hashmap，它的存放顺序并不是按照你存放的顺序来的，是乱序的，所以遍历的时候不能按照插入的顺序来遍历
		private static IntWritable outValue=new IntWritable();
		public void reduce(IntWritable key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
					int one=key.get();//第一个数
					String two="";//因为map的key不能重复，所以将第二个数字进行拼接
					for(IntWritable iw:values){
						count++;
						int t=iw.get()-one;//第二个数
						two+=t+":";
					}
					two=two.substring(0, two.length()-1);//去掉最后一个多出来的:
					System.out.println("map中存入："+one+"\t"+two);
					map.put(one, two);
					if(count==flag){//最后一次的时候进行输出 注意reduce处理次数要小于等于map，因为map后的结果做了归并
						//遍历map
						for(Map.Entry<Integer, String> e:map.entrySet()){//获取key-value映射set
							int a=e.getKey();//第一个数
							String[] bs = e.getValue().split(":");//第二个数的数组
							Arrays.sort(bs);//从小到大排序
							for(String i:bs){
								System.out.println("遍历map："+a+"\t"+i);
								context.write(new IntWritable(a), new IntWritable(Integer.parseInt(i)));//写入文件
							}
						}
					}
		}
	}
}
