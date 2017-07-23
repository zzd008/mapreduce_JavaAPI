package Dedup_examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//去重，去掉文件中重复的时间、ip
public class Dedup {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		conf.set("mapred.job.tracker","localhost:9001");
		String arg[]=new String[]{"/dedup_in","/dedup_out"};
		String otherArgs[]=new GenericOptionsParser(conf, arg).getRemainingArgs();
		if(otherArgs.length!=2){
			System.err.println("输入输出文件位置有误！");
			System.exit(1);
		}
		FileSystem fs=FileSystem.get(conf);
		if(fs.exists(new Path(arg[1]))){
			fs.delete(new Path(arg[1]), true);
		}
		
		Job job=new Job(conf,"Dedup_Job");
		job.setJarByClass(Dedup.class);//添加整个程序
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		//job.setCombinerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		/*job.setInputFormatClass(TextInputFormat.class); 输入输出格式类默认是TextInputFormat和TextOutputFormat，采用的RecordReader和RecordWriter默认是LineRecordReader和LineRecordWriter
		job.setOutputFormatClass(TextOutputFormat.class);*/
		FileInputFormat.addInputPath(job, new Path(arg[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg[1]));	
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
	public static class MyMapper extends Mapper<Object, Text, Text, Text>{//输出值为空字符串 
		@Override //重写父类方法，进行错误提示，可以不加
		public void map(Object key, Text value,Context context) throws IOException, InterruptedException {
			context.write(value, new Text(""));//输出：<"2014-10-3 10.3.2.5","">
		}
	}
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text>{
		@Override
		public void reduce(Text key, Iterable<Text> value,Context context) throws IOException, InterruptedException {
			context.write(key, new Text(""));//输入：<"2014-10-3 10.3.2.5",list["好多空字符"]> 输出：<"2014-10-3 10.3.2.5","">
		}
	}
}
