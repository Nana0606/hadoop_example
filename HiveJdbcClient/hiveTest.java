import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;


public class hiveTest {
	
	public static Connection getConnection(){
		Connection connection = null;
		try {
        //注意：hadoop-1.x.x和hadoop-2.x.x的驱动和地址不同，这里是hadoop-2.x.x的
        //192.168.163.131是hadoop集群的master的ip地址
		   	Class.forName("org.apache.hive.jdbc.HiveDriver");
		    connection = DriverManager.getConnection("jdbc:hive2://192.168.163.131:10000/default", "", "");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return connection;
	}
	
	public static void main(String[] args) throws Exception {
			
    Connection con = getConnection();
		Statement stmt = con.createStatement();
		String querySQL = "SELECT * FROM student";
		ResultSet res = stmt.executeQuery(querySQL);

		while (res.next()) {
			System.out.println(res.getString(1)+" " + res.getString(2));
		}

	}

}
