package MR_examples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//单词计数：记录文件中每个单词出现的次数
// 执行过程 Inputformat——》map——》map shuffle：（combine）——》partition分区——》sort——》merge——》reduce shuffle：copy——》merge——》（combine）——》sort——》reduce——》outputformat

//需要将core-site.xml hdfs-site.xml log4j.properties复制到src目录下 并刷新项目
//需要hdfs的jar包 
public class WordCount {//可以将map、reduce两个类分开写，也可以写成内部类
	//构造方法
	public void WordCount(){
		
	}
	//主方法
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		 Configuration con=new Configuration();//环境配置
		 String[] otherArgs = new GenericOptionsParser(con, args).getRemainingArgs();//存储程序运行时的参数
		 //String[] otherArgs=new String[]{"/input","/output"}; //直接设置输入参数 
		 if(otherArgs.length!=2){
			 System.err.println("运行参数错误，输入文件和输出文件！");//输出错误信息到控制台(红色字)
			 System.exit(2);// 退出程序 0--正常结束程序  非0--异常关闭程序；
		 }
		 Job job=new Job(con,"word count");//job 任务调度 设置环境参数
		 job.setJarByClass(WordCount.class);//设置整个程序的类对象
		 //java的每个类被编译成.class文件的时候，jvm都会自动为这个类生成一个类对象(类类)，这个对象保存了这个类的所有信息（成员变量，方法，构造器等），以后这个类要想实例化，那么都要以这个class对象为蓝图（或模版）来创建这个类的实例
		 //例如 User类 class<?> c=Class.forName("com.pojo.User");(通过完整路径去加载class类)  c就是User的类对象，而 User u=new User();这个u就是以c为模版创建的，其实就相当于u=c.newInstance(); 
		 //获取User类的class对象：User.class,  new User().getClass(),   Class.forName("com.pojo.User")
		 //获取User的实例：new User(),	User.class.newInstance(),	  Class.forName("com.pojo.User").newInstance() (class.newInstance())
		 job.setMapperClass(MyMapper.class);//添加MyMapper类(添加一个该类的类对象)
		 job.setReducerClass(MyReducer.class);//添加MyReducer类
		 job.setOutputKeyClass(Text.class);//设置输出key类型
		 job.setOutputValueClass(IntWritable.class);//设置输出value类型
		 FileInputFormat.addInputPath(job, new Path(otherArgs[0]));//设置输入文件 FileInputFormat、RecordReader、InputSplit
		 FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));//设置输出文件
		 System.exit(job.waitForCompletion(true)?0:1);//运行、并退出程序
	}
	//map类
	public static class MyMapper extends Mapper<Object,Text,Text,IntWritable>{//继承map类 泛型
		//输入类型<object,Text> 输出类型<Text,IntWritable>　除了object,其他都为hadoop内置类型
		//对两个输出变量one、word初始化  <word,one>即：<单词，次数>
		private static final IntWritable one=new IntWritable(1);//表示单词出现过一次
		private Text word=new Text();//单词
		//重写map函数
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{//前两个参数为map函数的输入　行号、每一行的值、存储中间结果
			System.out.println("map input key:"+key.toString()+"  value:"+value.toString());
			StringTokenizer st=new StringTokenizer(value.toString());//分值器　将value(即文本中的一行)进行拆分，默认以空格来分值 也可以自己来分割，用split
			while(st.hasMoreTokens()){//遍历分值器中拆分得到的值
				word.set(st.nextToken());//将每一个单词都存储到word中
				context.write(word, one);//Context是Map函数的一种输出方式,将中间结果存储在该变量中
				System.out.println("map output:<"+word+","+one+">");
			}//map后会执行partition ，它把不同的key放到不同的文件下，供不同的reduce来copy
		}
		
	}
	//reduce类
	 public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		 private IntWritable result=new IntWritable();
		 public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{//重写reduce方法
			 int sum=0;
			 for(IntWritable val:values){//IntWritable变量经过shuffle阶段处理后，变成了Iterable容器 <"a",<1,1,2>>
				 sum+=val.get();//次数累加
				 System.out.println("reduce input key:"+key.toString()+"  value:"+val.get());
			 }
			 result.set(sum);//将单词总次数放到result中
			 context.write(key, result);//存储信息
			 System.out.println("reduce output:<"+key.toString()+","+sum+">");
		 }
	 }
	
}
