package com.whs.dbi21.benchmark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

import com.whs.dbi21.benchmark.DbConnectionInfo;

public class Main {

	private static Connection dbCon;
	
	private static boolean initializeConnection() {
		try {
			dbCon = DriverManager.getConnection(DbConnectionInfo.JDBCSTRING, DbConnectionInfo.DBUSER, DbConnectionInfo.DBPASSWORD);
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}
	
	public static void main(String[] args) {
		if (!initializeConnection()) { 
			System.out.println("Could not establish connection to database!");
			System.exit(-1);
		}			
		System.out.println("Connection to database established!");		
		
		cleanDatabase();				
		System.out.println("Database cleaned!");
		
		System.out.println("Start benchmark test!");
		Date testStart = new Date();
		// Benchmark-Tasks durchführen		
		Date testFinish = new Date();
		
		long timeUsed = testFinish.getTime() - testStart.getTime();
		System.out.println("Time used in seconds = " + (double)timeUsed / 1000);		
	}

	private static boolean cleanDatabase() {
		Statement st;		
		try {
			boolean autoCommit = dbCon.getAutoCommit();
			
			dbCon.setAutoCommit(false);
			st = dbCon.createStatement();			
			st.addBatch("DELETE FROM accounts;");
			st.addBatch("DELETE FROM branches;");
			st.addBatch("DELETE FROM history;");
			st.addBatch("DELETE FROM tellers;");
			st.executeBatch();
			dbCon.commit();
			st.close();
			
			dbCon.setAutoCommit(autoCommit);
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}
}
