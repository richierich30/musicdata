package acadgild3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Shared_song{

private enum COUNTER {
INVALID_RECORD_COUNT
}

public static final int USER_ID=0;
public static final int TRACK_ID =1;
public static final int IS_SHARED=2;
public static final int RADIO=3;
public static final int IS_SKIPPED=4;

public static class Map extends Mapper<Object, Text, IntWritable, IntWritable> 
{
	IntWritable trackid = new IntWritable();
	IntWritable isShared = new IntWritable();
	
	public void map(Object key, Text value, Mapper<Object, Text, IntWritable, IntWritable>.Context context) throws IOException , InterruptedException
	{
		String[] a = value.toString().split("[|]");
		trackid.set(Integer.parseInt(a[Shared_song.TRACK_ID]));
		isShared.set(Integer.parseInt(a[Shared_song.IS_SHARED]));
		if(a.length == 5)
		{
			context.write(trackid, isShared);
		}
		else
		{
			context.getCounter(COUNTER.INVALID_RECORD_COUNT).increment(1L);
		}
	}
}
public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>
{
	private int count=0;
	public static final IntWritable t = new IntWritable();
	public void reduce(IntWritable trackid, Iterable<IntWritable> is_shared, Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)throws IOException,InterruptedException
	{
		for(IntWritable isShared: is_shared)
		{
			if(isShared.get() == 1) {
			count++;
		}
		}
	t.set(count);	
		context.write(trackid, t);
	}
	
}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 2)
		{
			System.err.println("Usage: songfully <in> <out>");
			System.exit(2);
		}
		@SuppressWarnings("deprecation")
		Job job = new Job(conf,"Number of times song was shared");
		job.setJarByClass(Shared_song.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
		
		org.apache.hadoop.mapreduce.Counters counter= job.getCounters();
		System.out.println("Number of Invalid Records: "+ counter.findCounter(COUNTER.INVALID_RECORD_COUNT).getValue());

	}

}
