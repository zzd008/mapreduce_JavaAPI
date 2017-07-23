package Sort_examples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
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

//将文件中的数字从小到大排列
public class Sort {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		conf.set("mapred.jpb.tracker", "localhost:9001");
		String arg[]=new GenericOptionsParser(conf, new String[]{"/sort_in","/sort_out"}).getRemainingArgs();
		if(arg.length!=2){
			System.err.println("in out file error!");
			System.exit(1);
		}
		FileSystem fs=FileSystem.get(conf);
		fs.delete(new Path(arg[1]), true);
		Job job=new Job(conf,"Sort_Job");
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		//job.setCombinerClass(SortReducer.class); 如果定义了combine，每次map结束后都要进行合并操作，这里对应的是SortReducer.class，所以每次map后都会执行reduce
		//如果没有定义combine，则map端全部执行完再执行reduce，不进行合并操作。。注意combine的作用及其对执行顺序的影响 combine等同于reduce
		job.setJarByClass(Sort.class);
		job.setOutputKeyClass(IntWritable.class);//注意输入输出类型匹配
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(arg[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
	public static class SortMapper extends Mapper<Object, Text, IntWritable, IntWritable>{
		private static IntWritable iw=new IntWritable();//因为输入文件中可能有空行，所以用\n过滤掉
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
				StringTokenizer st=new StringTokenizer(value.toString(),"\n");
				if(st.hasMoreElements()){
					String val=value.toString();
					iw.set(Integer.parseInt(val));
					context.write(iw,new IntWritable(1));
					System.out.println("map 输出："+iw.get()+"\t"+1);
				}
		}
	}	
	//map函数输出结果后，marpreduce有它自己的默认排序规则（用户也可以自定义combine函数），按照key值进项排序。
	//如果key为封装int的IntWritable类型，则按照数字大小排序。如果为封装String的Text类型，则按照字典顺序对字符串排列。排好序好，reduce函数按照顺序去取值
	
	public static class SortReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
		private static int n=1;
		public void reduce(IntWritable key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			for(IntWritable val:values){
				context.write(key, new IntWritable(n));
				//map后如果定义了combine，会去执行combine(合并)，待map结束后，reduce拿到了自己该拿的数据，就进行reduce函数
				//每个reduce函数会产生一个输出文件，输出文件的个数对应reduce任务的个数，多个输出文件不会进行合并，我们可以认为的去进行合并，我们可以设置reduce任务的个数 
				//这里是伪分布式，只有一个reduce，所以只有一个输出文件
				//reducer数量设置为1或者用hadoop fs -getmerge把一个目录下的文件合并成一个文件，这样就只有一个输出文件了
				n++;
				System.out.println("reduce 端输出："+key.get()+"\t"+(n));
			}
		}
	}
}
