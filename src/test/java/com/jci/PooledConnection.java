package com.jci;

import java.sql.Connection;

public class PooledConnection {
	
	private Connection conn;
    private boolean used;

    public PooledConnection(Connection conn){
        this.conn = conn;
        this.used = false;
    }

    public void setUsed(){
        this.used = true;
    }

    public void setFree(){
        this.used = false;
    }

    public boolean isUsed(){
        return this.used;
    }

    public Connection getConnection(){
        return this.conn;
    }
    
    

}
