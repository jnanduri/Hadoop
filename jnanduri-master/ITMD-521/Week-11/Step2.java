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
