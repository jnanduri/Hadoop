import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {
 private static final int MISSING =9999;String STN ="999999";
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString();
    String sid = line.substring(04, 10);
    int airTemperature=0,i = 1;
    if (line.charAt(87) == '+') {
       airTemperature = Integer.parseInt(line.substring(88,92));
   } else {
      airTemperature = Integer.parseInt(line.substring(87, 92));
   }
    String quality = line.substring(92, 93);
     if(!STN.equals(sid)){
      if (airTemperature == MISSING && quality.matches("[01459]")) {
      context.write(new Text(sid), new IntWritable(i));
    }

  }
}
}
~