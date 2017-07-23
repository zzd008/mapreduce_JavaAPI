package Index_examples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//从三个文件中生成带权重的倒序索引
public class Index {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		conf.set("mapred.job.tracker","localhost:9001");
		String arg[]=new GenericOptionsParser(conf, new String[]{"/index_in","/index_out"}).getRemainingArgs();
		if(arg.length!=2){
			System.err.println("input or output file error!");
			System.exit(2);
		}
		FileSystem fs=FileSystem.get(conf);
		if(fs.exists(new Path(arg[1]))){
			fs.delete(new Path(arg[1]), true);
		}
		
		Job job=new Job(conf, "Index_job");
		job.setJarByClass(Index.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setCombinerClass(Combine.class);
		
		//设置map的输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		//设置reduce的输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(arg[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg[1]));
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
	
	public static class MyMapper extends Mapper<Object,Text,Text,Text>{
		private Text key=new Text();//输出key
		private Text value=new Text();//输出value
		private FileSplit split;//split切片对象
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			split=(FileSplit) context.getInputSplit();//获取split对象
			StringTokenizer st=new StringTokenizer(value.toString());
			while(st.hasMoreTokens()){
				int splitIndex=split.getPath().toString().indexOf("D");//从完整路径中截取文件名
				this.key.set(st.nextToken()+":"+split.getPath().toString().substring(splitIndex));
				this.value.set("1");
				context.write(this.key, this.value);//key由单词和URL组成，value为单词频数 如<hello：D0.txt，1>
				System.out.println("map 输出："+this.key+"\t"+this.value);
			}
			
		}
	}

	public static class Combine extends Reducer<Text, Text, Text, Text>{//combine相当于reduce
		Text value=new Text();//输出value
		public  void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			int sum=0;
			for(Text t:values){//遍历，词频累加
				sum+=Integer.parseInt(t.toString());
			}
			int splitIndex=key.toString().indexOf(":");
			//重新设置输出的key、value 
			value.set(key.toString().substring(splitIndex+1)+":"+sum);
			key.set(key.toString().substring(0, splitIndex));
			context.write(key, value);//key为单词，value为URL和词频 如<hello,D0.txt:1>
			System.out.println("combine 输出："+key+"\t"+value);
		}
	}
	
	public static class MyReducer extends Reducer<Text,Text,Text,Text>{
		private Text result=new Text();//输出结果
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			//生成文档列表，对combine后的value进行合并
			String str=new String();
			for(Text t:values){
				str+=t.toString()+";";//连接
			}
			result.set(str);
			context.write(key,result);//key为单词，value为文档列表 如<hello,D2.txt:2;D0.txt:1>
			System.out.println("reduce 输出:"+key+"\t"+result);
		}
	}
}
