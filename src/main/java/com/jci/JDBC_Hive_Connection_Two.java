package com.jci;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.swing.JOptionPane;

import com.mchange.v2.c3p0.ComboPooledDataSource;



public class JDBC_Hive_Connection_Two {
	
	

	
	//  JDBC credentials
    static final String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    //static final String JDBC_DB_URL = "jdbc:hive2://c201sc02m002.jci.com:10010/default;principal=hive/c201sc02m002.jci.com@JCI.COM;auth=kerberos;kerberosAuthType=fromSubject";
    static final String JDBC_DB_URL = "jdbc:hive2://c201sc02m002.jci.com:10010/default;principal=hive/c201sc02m002.jci.com@JCI.COM;auth=kerberos;kerberosAuthType=fromSubject?hive.fetch.task.conversion=none";  //?hive.fetch.task.conversion=none
    //static final String JDBC_DB_URL = "jdbc:hive2://c201sc02m002.jci.com:2181,c201sc02m001.jci.com:2181,c201sc02m003.jci.com:2181/default;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;auth=kerberos;kerberosAuthType=fromSubject?hive.fetch.task.conversion=none";
    //static String QUERY = "show databases";
    //static String QUERY = "show tables";
    static String QUERY = "Select sc01001,sc01002,sc01003,sc01037,sc01066,sc01106,sc01120,sc01135 from iscala_bh_dbo_usable.sc01na00";// WHERE trim(lower(sc01001))='s1-02206844068";
    //static String QUERY = "select * from iscala_bh_dbo_usable.sl01na00";
    //"select * from iscala_bh_dbo_usable.CMH1NA00";
    //"select * from scala_nz_dbo_usable.sl01n100";

    static final String USER = null;
    static final String PASS = null;

    // KERBEROS Related.
    static final String KERBEROS_REALM = "JCI.COM";
    static final String KERBEROS_KDC = "JCI.COM";
    static final String KERBEROS_PRINCIPAL = "a3000053@JCI.COM"; //"hive/c201sc02m002.jci.com@JCI.COM";//
    static final String KERBEROS_PASSWORD = ""; //not mandatory. Any password works //782[-ca&]sanX7%YpGYa
    //static final String jaasConfigFilePath = "C:\\Users\\cdeyso\\Desktop\\RnD\\Eclipse Stuffs\\Sample Projects\\micro-service\\thrift-hive-spark\\src\\main\\java\\com\\zhili\\config\\r_jaas.conf"; //Change path. Find file bottom of the code. //keyTab="C:\ProgramData\MIT\Kerberos5\a3000053.keytab"
    static final String jaasConfigFilePath = "r_jaas.conf"; //Change path. Find file bottom of the code. //keyTab="C:\ProgramData\MIT\Kerberos5\a3000053.keytab"

    public static class MyCallbackHandler implements CallbackHandler {

            public void handle(Callback[] callbacks)
                            throws IOException, UnsupportedCallbackException {
                    for (int i = 0; i < callbacks.length; i++) {
                            if (callbacks[i] instanceof NameCallback) {
                                    NameCallback nc = (NameCallback)callbacks[i];
                                    nc.setName(KERBEROS_PRINCIPAL);
                            } else if (callbacks[i] instanceof PasswordCallback) {
                                    PasswordCallback pc = (PasswordCallback)callbacks[i];
                                    pc.setPassword(KERBEROS_PASSWORD.toCharArray());
                            } else throw new UnsupportedCallbackException
                            (callbacks[i], "Unrecognised callback");
                    }
            }
    }

    static Subject getSubject() {
            Subject signedOnUserSubject = null;

            // create a LoginContext based on the entry in the login.conf file
            LoginContext lc;
            try {
                    lc = new LoginContext("imasetup", new MyCallbackHandler());
                    
                    
                    // login (effectively populating the Subject)
                    lc.login();
                    
                    System.out.println("Test-----");
                    // get the Subject that represents the signed-on user
                    signedOnUserSubject = lc.getSubject();
            } catch (LoginException e1) {
                    // TODO Auto-generated catch block1
                    e1.printStackTrace();
                    System.exit(0);
            }
            return signedOnUserSubject;
    }
    
    
    public static ComboPooledDataSource getDataSource() throws PropertyVetoException
	{
    		//Using C3P0 for connection pooling and opening a connection.	
    		ComboPooledDataSource cpds = new ComboPooledDataSource();
			cpds.setDriverClass(JDBC_DRIVER); //Set Driver for Required Database
        	cpds.setJdbcUrl( JDBC_DB_URL ); // Set JDBC URL
        	cpds.setUser(USER);  // Set USER ID                               
        	cpds.setPassword(PASS); 

			// Optional Settings
			cpds.setInitialPoolSize(5);
			cpds.setMinPoolSize(5);
			cpds.setAcquireIncrement(5);
			cpds.setMaxPoolSize(20);
			cpds.setMaxStatements(100);
			
			

			return cpds;
	}

    static Connection getConnection( Subject signedOnUserSubject ) throws Exception{

            Connection conn = (Connection) Subject.doAs(signedOnUserSubject, new PrivilegedExceptionAction<Object>()
                            {
                    public Object run() 
                    {
                            Connection con = null;
                            Connection con1 = null;
                            Connection con2 = null;
                            try {
                                    /*Class.forName(JDBC_DRIVER);
                                    con =  DriverManager.getConnection(JDBC_DB_URL,USER,PASS);*/
                            	System.out.println("Inside getConnection !");
                            	ComboPooledDataSource dataSource = JDBC_Hive_Connection_Two.getDataSource();
                            	System.out.println("no of available connections :" + dataSource.getNumBusyConnections());
                            	Date connStartTime_getC_1 = new Date();
                            	con = dataSource.getConnection();
                            	Date connEndTime_getC_1 = new Date();
                            	System.out.println("no of available connections :" + dataSource.getNumBusyConnections());
                            	Date connStartTime_getC_2 = new Date();
                            	con1 = dataSource.getConnection(); //Second Connection that is pooled.
                            	Date connEndTime_getC_2 = new Date();
                            	System.out.println("no of available connections :" + dataSource.getNumBusyConnections());
                            	Date connStartTime_getC_3 = new Date();
                            	con2 = dataSource.getConnection(); //3rd Connection that is pooled.
                            	Date connEndTime_getC_3 = new Date();
                            	System.out.println("no of available connections :" + dataSource.getNumBusyConnections());
                            	
                            	long connDuration_1  = connStartTime_getC_1.getTime() - connEndTime_getC_1.getTime();
                                long connDuration_2  = connStartTime_getC_2.getTime() - connEndTime_getC_2.getTime();
                                long connDuration_3  = connStartTime_getC_3.getTime() - connEndTime_getC_3.getTime();
                                
                                
                                long diffInMillSec_conn = TimeUnit.MILLISECONDS.toMillis(connDuration_1);
                                long diffInSeconds_conn = TimeUnit.MILLISECONDS.toMillis(connDuration_2);
                                long diffInSeconds_conn_3 = TimeUnit.MILLISECONDS.toMillis(connDuration_3);
                            	
                            	
                            	System.out.println("Connection Duration Conn 1-----"+diffInMillSec_conn);
                                System.out.println("Connection Duration Conn 2-----"+diffInSeconds_conn);
                                System.out.println("Connection Duration Conn 3-----"+diffInSeconds_conn_3);
                                
                            	
                            	System.out.println("Exiting getConnection !");
                            	
                            } /*catch (ClassNotFoundException e) {
								// TODO: handle exception
                            	e.printStackTrace();
							}*/ catch (SQLException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (PropertyVetoException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
                            return con2;
                    }
                            });

            return conn;
    }

    // Print the result set.
    private static int  traverseResultSet(ResultSet rs, int max) throws SQLException
    {
            ResultSetMetaData metaData = rs.getMetaData();
            int rowIndex = 0;
            while (rs.next()) {
                    for (int i=1; i<=metaData.getColumnCount(); i++) {
                            System.out.print("  "  + rs.getString(i));
                    }
                    System.out.println();
                    rowIndex++;
                    if(max > 0 && rowIndex >= max )
                            break;
            }
            return rowIndex;
    }
    
    /*public static long getDateDiff(Date date1, Date date2, TimeUnit timeUnit) {
        long diffInMillies = date2.getTime() - date1.getTime();
        return timeUnit.convert(diffInMillies,TimeUnit.MILLISECONDS);
    }*/

    @SuppressWarnings("resource")
	public static void main(String[] args) {
            System.setProperty("java.security.auth.login.config", jaasConfigFilePath);
            System.setProperty("java.security.krb5.realm", KERBEROS_REALM );
            System.setProperty("java.security.krb5.kdc", KERBEROS_KDC);
            
            //System.setProperty("java.security.krb5.conf","krb5.conf"); //Not necessary 

            System.out.println("-- Test started ---");
            Subject sub = getSubject(); //Getting error on this file
            System.out.println("Subject ---"+sub);
            
            System.out.println("Principal ---- " + sub.getPrincipals());
          
            Connection conn = null;
            
            try {
            		System.out.println("Inside Main Method !");
            		Date connStartTime = new Date();
                    conn = getConnection(sub);
                    System.out.println("Connection in main method :"+conn);
                    Statement stmt = conn.createStatement() ;
                    System.out.println("Connection Established to HIVE SERVER 2!");
                    Date connEndTime = new Date();
                    
                    Date querryStartTime = new Date();
                    ResultSet rs = stmt.executeQuery( QUERY );
                    traverseResultSet(rs, 10);
                    
                    Date querryEndTime = new Date();
                    
                    long connDuration  = connStartTime.getTime() - connEndTime.getTime();
                    long querryDuration  = querryStartTime.getTime() - querryEndTime.getTime();
                    
                    long diffInMillSec_conn = TimeUnit.MILLISECONDS.toMillis(connDuration);
                    long diffInSeconds_conn = TimeUnit.MILLISECONDS.toSeconds(connDuration);
                    long diffInMillSec_querry = TimeUnit.MILLISECONDS.toMillis(querryDuration);
                    long diffInSeconds_querry = TimeUnit.MILLISECONDS.toSeconds(querryDuration);
                    
                    System.out.println("Query Executed  : "+QUERY);
                    System.out.println("Connection Start Time -----"+connStartTime);
                    System.out.println("Connection End Time -----"+connEndTime);
                    System.out.println("Connection Time Elapsed in millis -----"+diffInMillSec_conn+" millis");
                    System.out.println("Connection Time Elapsed in secs-----"+diffInSeconds_conn+" sec");
                    System.out.println("Execution Start Time -----"+querryStartTime);
                    System.out.println("Execution End Time -----"+querryEndTime);
                    System.out.println("Execution Time Elapsed in millis-----"+diffInMillSec_querry+" millis");
                    System.out.println("Execution Time Elapsed in secs-----"+diffInSeconds_querry+" sec");
                    
                    
            } catch (Exception e){
                    e.printStackTrace();
            } /*finally {
                    try { if (conn != null) conn.close(); } catch(Exception e) { e.printStackTrace();}
            }*/
            
            System.out.println("Test ended  ");
           // JOptionPane.showMessageDialog(null, "Completed!");
            Scanner scanner = new Scanner(System.in);
            System.out.print("Please enter your name: ");
            //String name = System.console().readLine();
            String name = scanner.next();
            if(name!= null){
            	main(args);
            }
            else{
            	JOptionPane.showMessageDialog(null, "Completed!");
                
            }
    }

}
