package Average_examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//求平均值的mapreduce驱动类，用于启动mr程序
public class Average {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "localhost:9001");//设置mr位置，这样就不用把配置文件放在文件目录下了
		
		String [] arg=new String[]{"/average_in","/average_out"};//设置输入输出文件，也可以在运行时设置
		String []otherArgs=new GenericOptionsParser(conf, arg).getRemainingArgs();//存储运行参数
		if(otherArgs.length!=2){//运行前判断合法性
			System.err.println("输入文件或输出文件不存在！");
			System.exit(2);
		}
		//如果输出文件存在，就先将它删除
		final FileSystem fs=FileSystem.get(conf);
		if(fs.exists(new Path(arg[1]))){
			fs.delete(new Path(arg[1]), true);//递归删除
		}
		
		Job job = Job.getInstance(conf, "Score Average");//也可以 new job();
		job.setJarByClass(Average.class);//设置类名
		// TODO: specify a mapper
		job.setMapperClass(AverageMap.class);//设置mapper处理类
		// TODO: specify a reducer
		job.setReducerClass(AverageReduce.class);//设置reducer处理类
		//job.setCombinerClass(new AverageReduce().getClass());//设置combine处理类 注意有无combine对执行顺序的影响
		//AverageReduce ar = AverageReduce.class.newInstance();
		
		// TODO: specify output types
		job.setOutputKeyClass(Text.class);//设置输出key类型
		job.setOutputValueClass(IntWritable.class);//设置输出value类型

		//将输入的数据集分割成split，指定使用哪种InputFormat输入格式类，及其RecordReader(RecordReader用来读取split中数据，最后转化为键值对交给map任务)
		job.setInputFormatClass(TextInputFormat.class);//使用TextInputFormat输入格式类，该类使用LineRecordReader，逐行读入，键为这行文本在文件中的字符偏移量，值为行中的文本，最后将键值对交给map任务
		job.setOutputFormatClass(TextOutputFormat.class);//设置输出格式类,通过实现一个RecordReader来负责数据输出
		
		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path(arg[0]));//设置输入输出目录
		FileOutputFormat.setOutputPath(job, new Path(arg[1]));

		if (!job.waitForCompletion(true))//提交job作业
			return;
//		System.exit(job.waitForCompletion(true)?0:1);
	}

}
