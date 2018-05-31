// cc MaxTemperatureMapper Mapper for maximum temperature example

// vv MaxTemperatureMapper
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTemperatureMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {

  private static final int MISSING = 9999;

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    int year=0;
    String line = value.toString();
    String years = line.substring(29, 34);
    String yeart="other";
        int yeari = Integer.parseInt(years);
        double i= yeari * 0.0001;
int intVal = (int) Math.floor(i);
        if (line.charAt(28) == '-')
        {
switch (intVal) {
    case 0 :
    {
yeart = "0-10";
year = 10;
        break;
    }
    case 1:
    {
        year = 20;
		yeart = "10-20";
        break;
    }
    case 2:
    {
        year = 30;
		yeart = "20-30";
        break;
    }
    case 3:
    {
	        year = 40;
			yeart = "30-40";
			break;
    }
    case 4:
    {
        year = 50;
		yeart = "40-50";
        break;
    }
    case 5:
    {
        year = 60;
		yeart = "50-60";
        break;
    }
    case 6:
    {
        year = 70;
		yeart = "60-70";
        break;
    }
    case 7:
    {
        year = 80;
		yeart = "70-80";
        break;
    }
    case 8:
    {
        year = 90;
		yeart = "80-90";
        break;
    }
    default:
        break;
}
}
    int airTemperature;
    if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
      airTemperature = Integer.parseInt(line.substring(88, 92));
    } else {
      airTemperature = Integer.parseInt(line.substring(87, 92));
    }
    String quality = line.substring(92, 93);
    if (airTemperature != MISSING && quality.matches("[01459]")) {
      context.write(new Text(yeart), new IntWritable(airTemperature));
    }