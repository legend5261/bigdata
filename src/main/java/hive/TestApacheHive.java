package hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class TestApacheHive {
	private static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
	
	private static final String HIVE_URL = "jdbc:hive2://jyibd70:10000/default";
	
	private static final String HIVE_USER = "hadoop";
	
	private static final String HIVE_PWD = "hadoop";
	
	public static void main(String[] args) throws Exception {
		Connection conn = null;
		String table = "test_first_login";
		String sql = "create table if not exists " + table
						+ " (name string, age int, height string, weight string) "
						+ " partitioned by (date string) "
						+ " ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' "
						+ " STORED AS ORC";
		sql = "show partitions " + table;
		try {
			Class.forName(HIVE_DRIVER);
			conn = DriverManager.getConnection(HIVE_URL,HIVE_USER,HIVE_PWD);
			Statement statement = conn.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			while (resultSet.next()) {
				System.out.println(resultSet.getString(1));
			}
		} finally {
			if (conn != null) conn.close();
			System.out.println("execute ");
		}
	}
	
}
