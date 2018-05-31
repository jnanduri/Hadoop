import java.io.IOException;

import com.cloudera.sqoop.lib.RecordParser.ParseError;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;

public class MaxWidgetId extends Configured implements Tool {

  public static class MaxWidgetMapper
      extends Mapper<LongWritable, Text, Text, FloatWritable> {

   

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      
    	 String line = value.toString();
    	 String[] parts = line.split(",");
    	 
    	
    	String widget = parts[1];
    	float price =  Float.parseFloat(parts[2]);
    	
    	context.write(new Text(widget),new FloatWritable(price));
    	
    }
    	

    
  }

  public static class MaxWidgetReducer
      extends Reducer<Text, FloatWritable,Text, FloatWritable > {

    public void reduce(Text widget,  Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {
    	float price = 0.0f; 
        int count = 0;
        for (FloatWritable value : values)
                    {
                                price += value.get();     
                                count+=1;
                    }
	try{
        context.write(widget, new FloatWritable(price/count));
}
catch (IOException ioe){
            ioe.printStackTrace();
        }
     
    }
  }

  public int run(String [] args) throws Exception {
    Job job = new Job(getConf());

    job.setJarByClass(MaxWidgetId.class);

    job.setMapperClass(MaxWidgetMapper.class);
    job.setReducerClass(MaxWidgetReducer.class);

    FileInputFormat.addInputPath(job, new Path("widgets"));
    FileOutputFormat.setOutputPath(job, new Path("maxwidget"));

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(FloatWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);

    job.setNumReduceTasks(1);

    if (!job.waitForCompletion(true)) {
      return 1; // error.
    }

    return 0;
  }

  public static void main(String [] args) throws Exception {
    int ret = ToolRunner.run(new MaxWidgetId(), args);
    System.exit(ret);
  }
}
