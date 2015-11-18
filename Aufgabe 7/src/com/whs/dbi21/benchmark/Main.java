package com.whs.dbi21.benchmark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import com.whs.dbi21.benchmark.DbConnectionInfo;

public class Main {

	private static Connection dbCon;
	
	private static boolean initializeConnection() {
		try {
			dbCon = DriverManager.getConnection(DbConnectionInfo.JDBCSTRING, DbConnectionInfo.DBUSER, DbConnectionInfo.DBPASSWORD);
			return true;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	
	public static void main(String[] args) {
		if (!initializeConnection()) { 
			System.out.println("Could not establish connection to database!");
			System.exit(-1);
		}			
		
				
	}
}
