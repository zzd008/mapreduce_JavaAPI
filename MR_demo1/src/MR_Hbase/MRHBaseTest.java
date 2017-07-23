package MR_Hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

//通过MR求Student中学生的平均年龄Age，将结果写到StudentAge表中
public class MRHBaseTest {
	//创建StudentAge表 
	public static void createTable() throws IOException{
		Configuration conf=HBaseConfiguration.create();
		conf.set("hbase.rootdir", "hdfs://Test:9000/hbase");
		Connection con=ConnectionFactory.createConnection(conf);
		Admin admin=con.getAdmin();
		TableName tableName=TableName.valueOf("StudentAge");
		HTableDescriptor htd=new HTableDescriptor(tableName);
		htd.addFamily(new HColumnDescriptor("avg_age"));//列族
		if(admin.tableExists(tableName)){
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		admin.createTable(htd);
		System.out.println("表创建成功！");
	}
	
	public static class Map extends TableMapper<Text, IntWritable>{//输出类型
		//使用TableInputFormat输入模板 将表中的每一行转化为mr使用的格式
		/* 有的时候不要写在外面，写在外面会公用的，因为只new一个对象
		 	private Text name=new Text();
			private IntWritable age=new IntWritable();
		*/
		//Map类只new 一个对象，然后多次调用map()方法
		public void map(ImmutableBytesWritable row,Result result,Context context) throws IOException, InterruptedException{//输入类型
			Text name=null;//很巧妙，先将它设置为空，取到值再赋值，这样就避免了单元格遍历时重复
			IntWritable age=null;//默认值为0
			for(Cell cell:result.rawCells()){//遍历每一行中的每一个单元格
				byte[] qualifier = CellUtil.cloneQualifier(cell);//获取单元格所在列
				byte[] value = CellUtil.cloneValue(cell);//获取单元格的值
				if(new String(qualifier).equalsIgnoreCase("Name")){//找到Name列 忽略大小写
					if(value!=null){
						name=new Text("student_avgage");//把map输出的key都设为age，作为行健
					}
				}
				if(Bytes.toString(qualifier).equals("Age")){//获取年龄
					if(value!=null){
						age=new IntWritable(Integer.parseInt(Bytes.toString(value)));
					}
				}
			}
				if(name!=null&&age!=null){
					context.write(name, age);
					System.out.println("map 输出："+name+"\t"+age);
				}
		}
	}
	
	public static class Reduce extends TableReducer<Text, IntWritable, ImmutableBytesWritable>{
		private int sum=0;
		private int count=0;
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
				for(IntWritable iw:values){
					sum+=iw.get();
					count++;
					System.out.println("reduce 输入："+key+"\t"+iw);
				}
				int avgAge=sum/count;
				Put put=new Put(key.getBytes());
				put.addColumn("avg_age".getBytes(), "age".getBytes(), String.valueOf(avgAge).getBytes());
				context.write(null, put);
				System.out.println("写入表中：" +key+"\t"+avgAge);
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//先创建表
		createTable();
		Configuration conf=new Configuration();
		conf.set("mapred.job.tracker", "hdfs://localhost:9001");
		Job job=new Job(conf);
		job.setJobName("zzd_job");
		job.setJarByClass(MRHBaseTest.class);
		
		//因为MR要从表中读取每一行数据所以要定义scan
		Scan scan=new Scan();
		scan.addFamily("info".getBytes());//扫描info列族
		scan.setCaching(500);//设置每次RPC的请求记录数，因为MR要通过RPC访问表，默认为1，太小了，需要访问多次，对MR不利。设置为500 大一点，减少RPC次数来能提高性能
		scan.setCacheBlocks(false);//MR中为false 设置MR从hbase中读取的数据不放到缓存中，因为这些scan到的数据一般是一次性的，用过之后不会再使用
		
		//设置map
		/**
		 * 当map或reduce类继承了TableMapper或TableReducer，默认采用的输入输出格式类为TableInputFormat、TableOutputFormat,就不用在job中再去设置了
		 * 输入表名，scan，map类，map输出key类型，map输出value类型，job
		 */
		TableMapReduceUtil.initTableMapperJob("Student", scan, Map.class, Text.class, IntWritable.class, job);
		//设置reduce
		/**
		 * 输出表名，reduce类，job
		 */
		TableMapReduceUtil.initTableReducerJob("StudentAge", Reduce.class, job);
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
	@org.junit.Test
	public void Test(){
		try {
			createTable();
		} catch (IOException e) {
			System.out.println("error！");
			e.printStackTrace();
		}
	}
}
