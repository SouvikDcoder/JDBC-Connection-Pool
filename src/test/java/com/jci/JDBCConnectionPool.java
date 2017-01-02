/**
 * 
 */
package com.jci;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author cdeyso
 *
 */
public interface JDBCConnectionPool {
	
	public Connection getConnection() throws SQLException;
	
	public void releaseConnection(Connection conn);

}
