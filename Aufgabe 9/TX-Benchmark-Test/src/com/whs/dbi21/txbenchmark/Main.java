package com.whs.dbi21.txbenchmark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Main {

	private static Connection dbCon;
	
	private static void initializeConnection() throws SQLException {
		dbCon = DriverManager.getConnection(DbConnectionInfo.JDBCSTRING, DbConnectionInfo.DBUSER, DbConnectionInfo.DBPASSWORD);
	}
	
	public static void main(String[] args) {
		// Herstellen der Verbindung zur Datenbank		
		try {
			initializeConnection();
			System.out.println("Connection to database established!");		
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println("\nCould not establish connection to database!");			
			System.exit(-1);
		}			
		
		try {
			StatementsGenerator.executeInpaymentTx(100, 5, 5, 111, dbCon);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
