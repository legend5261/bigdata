package hive;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TestClouderaHive {
	
	public static void main(String[] args) throws SQLException {
		Connection conn = null;
		HiveAction action = HiveAction.getInstance();
		try {
			conn = action.getConnection();
			String table = "test_first_login";
			String sql = "show partitions " + table;
			ResultSet resultSet = conn.createStatement().executeQuery(sql);
			while (resultSet.next()) {
				System.out.println(resultSet.getString(1));
			}
		} finally {
			action.releaseConnection(conn, null, null);
			System.out.println("ok");
		}
	}
}
