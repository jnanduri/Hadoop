MaxWidgetId program run Result:

![step-5](https://user-images.githubusercontent.com/26098043/32695475-260225ac-c723-11e7-93ac-ef0df42fd567.jpg)

Average Price of each Widget Result:

![step-6](https://user-images.githubusercontent.com/26098043/32695494-8a583b68-c723-11e7-9445-0dd5f96288cc.jpg)

[outputavg.txt](https://github.com/illinoistech-itm/jnanduri/files/1464339/outputavg.txt)
[output_5000_records.txt](https://github.com/illinoistech-itm/jnanduri/files/1464340/output_5000_records.txt)
[output_2000_records.txt](https://github.com/illinoistech-itm/jnanduri/files/1464341/output_2000_records.txt)

_*Java Script to insert 5000 Random records*_
-------------------------------------------------

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public class Step2 {

	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
	static final String DB_URL = "jdbc:mysql://localhost:3306/hadoopguide";

	// Database credentials
	static final String USER = "root";
	static final String PASS = "itmd521";
	private static String NULL;

        
	public static void main(String[] args) throws SQLException, InstantiationException, IllegalAccessException {
		
        try {
			Class.forName(JDBC_DRIVER).newInstance();
		} catch (ClassNotFoundException e) {
			                                       // TODO Auto-generated catch block
			e.printStackTrace();
		}
                                                   // STEP 3: Open a connection
        System.out.print("\nConnecting to database...");
        Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
        System.out.println(" SUCCESS!\n");
                                                   // STEP 5: Execute query
        System.out.print("\nInserting records into table...");
                                                   
        
        for(int i = 4;i<=5000;i++)
        {	
        String widget_name = null;
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT widget_name FROM widgets ORDER BY RAND() LIMIT 1;");
        
        if(rs.next()){
        	 widget_name = rs.getString("widget_name");  
        	}
        String design_date = null;
        ResultSet rd = stmt.executeQuery("SELECT design_date FROM widgets ORDER BY RAND() LIMIT 1;");
		if(rd.next()){
       	 design_date = rd.getString("design_date");  
       	}
       
		System.out.print("\nInserting " +i+" records into table...");
        
        String SQL = "INSERT INTO widgets " + "VALUES (NULL, ? , RAND()*99.99, ? ,FLOOR(RAND()* 13), CONV(FLOOR(RAND() * 99999999999999), 20, 36));";
		PreparedStatement p = conn.prepareStatement(SQL);
			
		p.setString(1, widget_name);
		p.setString(2, design_date);
		p.executeUpdate();
		p.close();
        }
			
			
			
        System.out.println(" SUCCESS!\n");
        conn.close();}
		}
		
_*Java Script to find average price for each widgete*_
-------------------------------------------------
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






	
