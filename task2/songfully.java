package acadgild1;
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


public class songfully {

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
	IntWritable isSkipped = new IntWritable();
	
	public void map(Object key, Text value, Mapper<Object, Text, IntWritable, IntWritable>.Context context) throws IOException , InterruptedException
	{
		String[] a = value.toString().split("[|]");
		trackid.set(Integer.parseInt(a[songfully.TRACK_ID]));
		isSkipped.set(Integer.parseInt(a[songfully.IS_SKIPPED]));
		if(a.length == 5)
		{
			context.write(trackid, isSkipped);
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
	public void reduce(IntWritable trackid, Iterable<IntWritable> is_skipped, Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)throws IOException,InterruptedException
	{
		for(IntWritable isSkipped: is_skipped)
		{
			if(isSkipped.get() == 1) {
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
		Job job = new Job(conf,"Number of times song was heard fully");
		job.setJarByClass(songfully.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setCombinerClass(Reduce.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
		
		org.apache.hadoop.mapreduce.Counters counter= job.getCounters();
		System.out.println("Number of Invalid Records: "+ counter.findCounter(COUNTER.INVALID_RECORD_COUNT).getValue());

	}

}
