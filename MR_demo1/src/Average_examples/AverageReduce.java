package Average_examples;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//求平均值的reduce类
public class AverageReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		// process values
		int sum=0;
		int count=0;
		//获取迭代器
		Iterator<IntWritable> it=values.iterator();
		while(it.hasNext()){
			int n=it.next().get();
			System.out.println("reduce 输入："+key.toString()+"   "+n);
			sum+=n;//计算总分
			count++;//计算总科目数
		}
		int avg=(int)(sum/count);//计算平均成绩
		context.write(key, new IntWritable(avg));//输出
		System.out.println("reduce 输出："+key.toString()+"   "+avg);
	}

}
