package com.whs.dbi21.benchmark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.Scanner;

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
		
		System.out.print("Please enter a number for n: ");
		int n = readNumberInInt();
		if (n < 1) {
			System.out.println("No valid number for n was entered, using default: 10");
			n = 10;
		}
		
		
		System.out.println("Start benchmark test!");
		Date testStart = new Date();
		Statements.createdb(n, dbCon);	
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
			st.addBatch("DELETE FROM history;");
			st.addBatch("DELETE FROM accounts;");					
			st.addBatch("DELETE FROM tellers;");
			st.addBatch("DELETE FROM branches;");
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
	
    private static int readNumberInInt() {
        Scanner scanner = new Scanner(System.in);
        String s = scanner.next();
        try {
            int i = Integer.parseInt(s);
            scanner.close();
            return i;
        } catch (NumberFormatException e) {
        	scanner.close();
            return -1;
        }
        
    }
}
