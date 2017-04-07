package hive;

//import com.cloudera.hive.jdbc41.HS2DataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveAction {
	/** Hive HAProxy */
	private static final String HAPROXY_URL = "jdbc:hive2://jyibd42:10998/default;UseNativeQuery=1";
	
	private DataSource ds;

	private static HiveAction self = new HiveAction();
	
	public static HiveAction getInstance() {
		return HiveAction.self;
	}
	
	private HiveAction() {
		this.ds = setupDataSource();
	}
	
	public DataSource setupDataSource() {
		/*HS2DataSource hs2 = new HS2DataSource();
		hs2.setURL(HAPROXY_URL);
		hs2.setUserID("hadoop");
		hs2.setPassword("");
		return hs2;*/
		return null;
	}
	
	public Connection getConnection() {
		/*HS2DataSource ds = (HS2DataSource) getDs();
		try {
			return ds.getPooledConnection().getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		}*/
		return null;
	}
	
	protected void releaseConnection(Connection conn, Statement stmt,ResultSet rs) {
        if (null != conn) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (null != stmt) {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

	public DataSource getDs() {
		return ds;
	}

	public void setDs(DataSource ds) {
		this.ds = ds;
	}
}
