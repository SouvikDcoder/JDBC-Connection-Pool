package com.jci;

import java.util.Vector;

import com.jci.TestConnPoolOne;

public class TestConnPoolMain {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
	Vector<PooledConnection> connections = new Vector<PooledConnection>();
	
	//connections
	
	String url= "jdbc:hive2://c201sc02m002.jci.com:10010/default;principal=hive/c201sc02m002.jci.com@JCI.COM;auth=kerberos;kerberosAuthType=fromSubject?hive.fetch.task.conversion=none";
	String username = null;
	String password = null;
	TestConnPoolOne poolone = new TestConnPoolOne(connections, url, username, password);
	System.out.println(poolone.toString());

	}

}
