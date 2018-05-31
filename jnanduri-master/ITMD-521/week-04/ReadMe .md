#Jayanth 

#output 

<img width="650" alt="2 jpg" src="https://user-images.githubusercontent.com/26098043/30783485-503009d8-a109-11e7-813e-b433e5242a91.PNG">

<img width="665" alt="1sql" src="https://user-images.githubusercontent.com/26098043/30783486-5674a5a6-a109-11e7-86cb-435b8be3560b.PNG">

![output](https://user-images.githubusercontent.com/26098043/30783472-018d38f0-a109-11e7-9647-3a4f16049332.JPG)


#Instructions 

Install mysql server and set the user name as 'root' and  password as '1234'.
Copy the Input files to the home location Jna/ncdc.
Create two tables where we can insert the combined years data.
Then install JDBC  driver and set connection.
Run the java program to process the data and excute the MaxTemperature function to get the maximum temperature from the combined years data.






#code 
 SHELL SCRIPT TO INSTALL MYSQL
   sudo apt-get install mysql-server
   sudo service mysql restart
   mysql -u root -p
   
 #CREATING A TABLE
 
 
 CREATE TABLE ncdc2 (US_ID VARCHAR(20), WBAN VARCHAR(20), 
 YEAR VARCHAR(20), MONTH VARCHAR(20), DAY VARCHAR(20),
 TIME VARCHAR(20), LATITUDE VARCHAR(20), temp int(10));
 
 CREATE TABLE NCDC (US_ID VARCHAR(20), WBAN VARCHAR(20), 
 YEAR VARCHAR(20), MONTH VARCHAR(20), DAY VARCHAR(20),
 TIME VARCHAR(20), LATITUDE VARCHAR(20), temp int(10));

 #JAVA FOR PROCESSING THE DATA
 
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public class readingtextfile {

	
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
	static final String DB_URL = "jdbc:mysql://localhost:3306/MYDB";

	// Database credentials
	static final String USER = "root";
	static final String PASS = "root";

        
	public static void main(String[] args) {
		String FILENAME= args[0];
		File file=new File(FILENAME);
        try
        {
        	FileReader fr = new FileReader(FILENAME);
			BufferedReader br = new BufferedReader(fr);
			String s;
		// STEP 2: Register JDBC driver
        Class.forName("com.mysql.jdbc.Driver");
        // STEP 3: Open a connection
        System.out.print("\nConnecting to database...");
        Connection conn = DriverManager.getConnection(DB_URL, USER, PASS);
        System.out.println(" SUCCESS!\n");
     // STEP 5: Execute query
        System.out.print("\nInserting records into table...");
        Statement stmt = conn.createStatement();
        while ((s = br.readLine()) !=null) 
        {
        	String US_ID= s.substring(0,10);
			String WBAN= s.substring(10,15);
			String YEAR= s.substring(15,19);
			String MONTH=s.substring(19,21);
			String DAY= s.substring(21,23);
			String TIME= s.substring(23,28);
			String LATITUDE= s.substring(28,34);
			String airtemp= s.substring(87,92);
			String TEMP;
			String b= "+9999";
			String c="0";
			String d;
			int temp= 0;
			if(airtemp.equals(b))
			{
				TEMP = c;
			}
			else
			{
				TEMP = airtemp;
			}
			if(Integer.parseInt(TEMP) >= 0)
				
			{
				temp =Integer.parseInt(TEMP);  
			}
			d=Integer.toString(temp);
			System.out.println("changed value"+TEMP);
		//	String sql1="CREATE TABLE ncdc("+"US_ID VARCHAR(20), "
			//                               +"WBAN VARCHAR(20), "
			  //                             +"YEAR VARCHAR(20), "
			    //                           +"MONTH VARCHAR(20), "
			      //                         +"DAY VARCHAR(20), "
			        //                       +"TIME VARCHAR(20), "
			          //                     +"LATITUDE VARCHAR(20), "
			            //                   +"TEMP VARCHAR(20), "+")";
		//	stmt.executeUpdate(sql1);
       // String sql = " INSERT INTO ncdc" +
        //		"VALUES ('1', '2', '3', '4', '5','6', '7', '8')";
            //"VALUES (US_ID, WBAN, YEAR, MONTH, DAY,TIME, LATITUDE, temp)";
        //stmt.executeUpdate(sql);
			
			String SQL = "INSERT INTO ncdc2 VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
			PreparedStatement p = conn.prepareStatement(SQL);
			p.setString(1, US_ID);
			p.setString(2, WBAN);
			p.setString(3, YEAR);
			p.setString(4, MONTH);
			p.setString(5, DAY);
			p.setString(6, TIME);
			p.setString(7, LATITUDE);
			p.setString(8, d);
			p.executeUpdate();
			p.close();
			
			
        }
        //String sql="SELECT MAX(temp) AS temp FROM ncdc2;";
        //System.out.println("MAX TEMPERATURE"+stmt.executeUpdate(sql));
        
        System.out.println(" SUCCESS!\n");
        conn.close();
		
        }
        catch(Exception e){ System.out.println(e);}
        	
		}
}


   


 #CALCULATING THE MAXIMUM TEMPERATURE 

    SELECT MAX(temp) AS temp FROM ncdc2;