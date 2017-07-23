package SecondrySort_examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import SecondrySort_examples.SecondrySort.SecondrySortMapper;
import SecondrySort_examples.SecondrySort.SecondrySortReducer;

//二次排序
public class SecondrySort1 {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		conf.set("mapred.jpb.tracker", "localhost:9001");
		String arg[]=new GenericOptionsParser(conf, new String[]{"/secondrysort_in","/secondrysort1_out"}).getRemainingArgs();
		if(arg.length!=2){
			System.err.println("in out file error!");
			System.exit(1);
		}
		FileSystem fs=FileSystem.get(conf);
		fs.delete(new Path(arg[1]), true);
		
		Job job=new Job(conf,"SecondrySort1_job");
		job.setJarByClass(SecondrySort1.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		//设置分组函数类 可以不用指定分组类，因为MR有默认的分组操作，也就是文件归并，归并时会将具有相同的key的多个value归并成一个迭代器 <"a",1>,<"a",2>---><"a",<1,2>>
		//job.setGroupingComparatorClass(GroupingComparator.class);
		
		job.setMapOutputKeyClass(IntPair.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//job.setSortComparatorClass(null);map阶段后，会调用partitioner对输出的list进行分区，每个分区映射一个reduce
		//每个分区又调用job.setSortComparatorClass()来设置对key的排序，若没有通过job.setSortComparatorClass()来设置比较类s，则使用key的compareTo()方法排序
		
		FileInputFormat.addInputPath(job, new Path(arg[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
	
	/**
	 * 创建新主键类（也就是自定义一种输入输出类型），把第一列整数和第二列整数作为类的属性，并实现WritableComparable接口
	 * 如Text封装了String类型，IntWritable封装了int类型，LongWritable封装了long类型
	 * @author zzd
	 *MR中所有的key和value都要实现Writable接口，用于读（反序列化）和写（序列化）、传输、排序等
	 *在MapReduce的过程中，需要对key进行排序，而key也需要在网络流中传输，因此需要实现WritableComparable，因此key实现了Writable, Comparable两个接口。
	 */
	public static class IntPair implements WritableComparable<IntPair>{
		private int first=0;
		private int second=0;
		
		//get,set方法
		public void set(int left,int right){
			first=left;
			second=right;
		}
		public int getFirst(){
			return first;
		}
		public int getSecond(){
			return second;
		}
		
		//@Override实现接口中方法不用加，重写父类方法时可以加，jdk1.6之后无论是实现接口还是重写方法都可以加
		public void readFields(DataInput in) throws IOException {//读
			first=in.readInt();
			second=in.readInt();
		}
		
		public void write(DataOutput out) throws IOException {//写
			out.writeInt(first);
			out.writeInt(second);
		}

		public int compareTo(IntPair arg) {//如果用户没有通过job.setSortComparatorClass()来设置排序类，则默认调用这个方法来对key排序
			if(first!=arg.first){//先比较第一个数，然后比较第二个数 如(20,21)排在（20,22）前面
				return first-arg.first;//正数表示大，负数表示小，零表示相等，MR一定会根据这个差值来排序的。。。
			}
			else if(second!=arg.second){
				return second-arg.second;
			}
			else return 0;
		}
		
	}
	
	/**
	 * 分组类
	 * 在reduce中，一个key对应一个value迭代器，在这之前，要用到分组，也相当于归并。
	 * 如果比较的两个key相同，那么他们就属于一组没他们的value就在同一个迭代器里
	 * 通过job.setGroupingComparatorClass()来指定分组类
	 * 可以不用指定分组类，因为MR有默认的分组操作，也就是文件归并，归并时会将具有相同的key的多个value归并成一个迭代器 <"a",1>,<"a",2>---><"a",<1,2>>
	 * @author hadoop
	 *
	 */
	public static class GroupingComparator implements RawComparator<IntPair>{

		public int compare(IntPair arg0, IntPair arg1) {
			int first=arg0.getFirst();//分组时只比较key
			int second=arg1.getSecond();
			return second-first;
		}

		public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3,
				int arg4, int arg5) {
			return WritableComparator.compareBytes(arg0, arg1, Integer.SIZE/8, arg3, arg4, Integer.SIZE/8);//不知道啥意思
		}
		
	}
	
	
	public static class MyMapper extends Mapper<LongWritable, Text, IntPair, IntWritable>{
		private final IntPair key=new IntPair();
		private final IntWritable value=new IntWritable();
		@Override
		public void map(LongWritable inKey,Text inValue,Context context) throws IOException, InterruptedException{
			StringTokenizer st=new StringTokenizer(inValue.toString());
			if(st.hasMoreTokens()){
				int left=0;
				int right=0;
				left=Integer.parseInt(st.nextToken());
				if(st.hasMoreTokens()){
					right=Integer.parseInt(st.nextToken());
				}
				key.set(left, right);
				value.set(right);
				context.write(key, value);//map输出：<IntPair（两个数），第二个数>
				System.out.println("map 输出："+key+"\t"+value);
			}
		}
	}
	
	public static class MyReducer extends Reducer<IntPair,IntWritable,Text,IntWritable>{
		private static final Text Hr=new Text("-----------------");
		private final Text first=new Text();
		
		public void reduce(IntPair key,Iterable< IntWritable> values,Context context) throws IOException, InterruptedException{
			//reduce的输入都是对key排好序的
			context.write(Hr, null);//输出分割线
			first.set(Integer.toString(key.getFirst()));
			for(IntWritable iw:values){
				context.write(first, iw);//reduce输出： <第一个数，第二个数>
				System.out.println("reduce 输出："+first+"\t"+iw);
			}
		}
	}
}
