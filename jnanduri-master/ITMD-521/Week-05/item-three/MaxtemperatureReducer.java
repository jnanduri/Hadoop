import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTemperatureReducer
  extends Reducer<Text, IntWritable, Text, IntWritable>
{
  int tot=0; Text maxWord = new Text();
  int maxValue = Integer.MIN_VALUE;
  @Override
  public void reduce(Text key, Iterable<IntWritable> values,
      Context context)
      throws IOException, InterruptedException
{
    for (IntWritable value : values)
{       tot = tot + value.get(); }
 if(tot > maxValue)
    {
        maxValue = tot;
        maxWord.set(key);
    }
}
    public void cleanup(Context context) throws IOException, InterruptedException
        {
        context.write(maxWord,new IntWritable(maxValue));
        }
 }