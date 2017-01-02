package com.jci;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Vector;

public class TestConnPoolOne implements JDBCConnectionPool{
	
	
	private Vector<PooledConnection> connections; // The connections container
    String url;
    String username; 
    String password;
    
    
	/**
	 * @param connections
	 * @param url
	 * @param username
	 * @param password
	 */
	public TestConnPoolOne(Vector<PooledConnection> connections, String url, String username, String password) {
		super();
		this.connections = connections;
		this.url = url;
		this.username = username;
		this.password = password;
	}

	
    
    

	@Override
	public Connection getConnection() throws SQLException {
		// TODO Auto-generated method stub
		synchronized (this.connections) {
			
			// Checking if there is an available connection to return
            for(PooledConnection c : this.connections){
                if(!c.isUsed()){
                    c.setUsed();
                    return c.getConnection();
                }
            }
            
            // If there are none, open a new one and return it
            Connection conn = DriverManager.getConnection(url, username, password);
        PooledConnection pConn = new PooledConnection(conn);
        pConn.setUsed();
        connections.add(pConn);
        System.out.println(pConn.getConnection());
        return pConn.getConnection();
		}
		
		
	}

	/**
     * Releases a connection to the pool.
     * 
     * @param con the connection to release.
     */
	
	@Override
	public void releaseConnection(Connection conn) {
		// TODO Auto-generated method stub
		
		synchronized(this.connections){
            for(PooledConnection c : this.connections){
                if(c.getConnection().equals(conn)){
                    c.setFree();
                    return;
                }
            }
        }
		
	}

}
