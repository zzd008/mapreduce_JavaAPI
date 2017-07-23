package Average_examples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//可以分开写，也可以写成内部类
//求平均值的map类
public class AverageMap extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable ikey, Text ivalue, Context context)//ikey为传输的key，ivalue为传入的value
			throws IOException, InterruptedException {
			String line=ivalue.toString();//将输入的纯文本文件转化成string
			System.out.println("map输入："+ikey+"\t"+line);
//			StringTokenizer st=new StringTokenizer(line,"\n");//分值器,以回车来分割
			//对每一行进行处理
//			while(st.hasMoreElements()){   可以不用\n来进行分割，多此一举，因为每次只输入一行，不用再去用\n分割了 分割后的一个单词叫Token 一行中多个单词叫Elements
				StringTokenizer st1=new StringTokenizer(line);//每行按空格划分 
				while(st1.hasMoreTokens()){//这句话一定要加，hasMoreTokens()指向下一个token
					String strName=st1.nextToken();//姓名
					String strScore=st1.nextToken();//成绩
					Text name=new Text(strName);//输出类型
					int score1=Integer.parseInt(strScore);//将成绩转化成int
					IntWritable score=new IntWritable(score1);
					context.write(name, score);//输出成绩和姓名到本地磁盘
					System.out.println("map 输出："+name+"\t"+score);
				}
//			}
	}

}
