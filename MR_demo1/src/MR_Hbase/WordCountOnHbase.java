package MR_Hbase;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

//日了狗了，hbase里的jar包不能放在user library里面，不然报错！！
//而且同时倒进hdfs和hbase的jar包，会版本冲突：SLF4J: Class path contains multiple SLF4J bindings.
//MR中使用hbase：统计java文件中各个类的使用频率
public class WordCountOnHbase {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		createHbaseTable();//先建表
		Configuration conf=new Configuration();
		conf.set("mapred.job.tracker", "hdfs://localhost:9001");
		//设定reduce的输出到哪个表,现在用TableMapReduceUtil类的initTableMapperJob()或initTableReducerJob()方法来代替
		conf.set(TableOutputFormat.OUTPUT_TABLE, "wordcount");
		Job job=new Job(conf,"mapreduce on hbase");
		job.setJarByClass(WordCountOnHbase.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);//设置reduce任务数
		//设置输入输出所使用的格式类
		job.setInputFormatClass(TextInputFormat.class);//默认
		job.setOutputFormatClass(TableOutputFormat.class);//如果map继承类tablemapper或reduce继承了tablereducer，则默认输出格式类是这个
		//将输入文件上传到hdfs中
		Configuration con=new Configuration();
		con.set("fs.defaultFS", "hdfs://localhost:9000");
		FileSystem fs=FileSystem.get(con);
		String dirPath="/MRHBase_in";
		if(fs.exists(new Path(dirPath))){//在hdfs上创建输入文件夹
			fs.delete(new Path(dirPath), true);
			fs.mkdirs(new Path(dirPath));
		}
		FileInputStream in=new FileInputStream("/home/hadoop/workspace/MR_demo1/src/MR_examples/WordCount.java");//本地java文件路径
		FSDataOutputStream out = fs.create(new Path(dirPath+"/WordCount.java"));//hdfs上文件的路径
		IOUtils.copyBytes(in, out, con);//流复制
		
		FileInputFormat.addInputPath(job,new Path(dirPath));
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
		private IntWritable one=new IntWritable(1);
		public void map(LongWritable inKey,Text inValue,Context context) throws IOException, InterruptedException{
			//用正则表达式获取每行中的单词、数字  [^\\w]表示一个非单词字符 trim()用于去掉字符串首尾的空格
			String[] strs = inValue.toString().trim().split("[^\\w]");
			//StringTokenizer st=new StringTokenizer(inValue.toString().trim(),"[^\\w]");
			for(String s:strs){
				if(s.matches("[A-Z]\\w*")){//匹配类名 以大写字母开头并包含一些小写字母
					context.write(new Text(s), one);
					System.out.println("map 输出："+s+"\t"+1);
				}
			}
		}
	}
	
	//很尴尬 不能继承Reducer，因为里面不能写关于hbase的操作，要继承TableReducer
	public static class Reduce extends TableReducer<Text, IntWritable, NullWritable>{//输出类型为NullWritable，也可以为ImmutableBytesWritable，写到数据库
		public void reduce(Text inKey,Iterable<IntWritable> inValues,Context context) throws IOException, InterruptedException{
			int sum=0;
			for(IntWritable iw:inValues){
				sum+=iw.get();//统计次数
			}
			//写入表中
			Put put=new Put(inKey.toString().getBytes());
			put.addColumn("result".getBytes(), Bytes.toBytes("connt"), Integer.toString(sum).getBytes());
			//TableReducer输出的value只能是put或delete key一般为nullwritable
			context.write(NullWritable.get(), put);
			System.out.println("写入数据库 ："+inKey+"\t"+sum );
		}
	}
	
	//创建存放统计结果的表
	//行号为类名，每一行只有一个列名“result：count”，用于存放频数
	public static void createHbaseTable() throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		Configuration conf=HBaseConfiguration.create();//hbase里的jar包不能放在user library里面，要直接导进来，不然报错！！
		conf.set("hbase.rootdir", "hdfs://Test:9000/hbase");
		//HBaseAdmin admin=new HBaseAdmin(conf);过期
		Connection con=ConnectionFactory.createConnection(conf);
		Admin admin=con.getAdmin();
		
		String tableName="wordcount";
		String columnFamily="result";
		TableName tbName=TableName.valueOf(tableName);
		HTableDescriptor htd=new HTableDescriptor(tableName);
		htd.addFamily(new HColumnDescriptor(columnFamily));
		
		if(admin.tableExists(tbName)){
			admin.disableTable(tbName);
			admin.deleteTable(tbName);
		}
		admin.createTable(htd);
	}
}
