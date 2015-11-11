package de.whs.dbi21;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Main {

	private static Connection dbCon = null;
	
	public static void main(String[] args) {
		try {
			initiateConnection();
			while(true) {
				System.out.println("--------------------------------------------------------------------");
				System.out.print("Produktid eingeben:  ");				
				String product = readIn();
				System.out.println("");
				if (!product.isEmpty())
					printOutUmsatzProdukt(product);
			}
		} catch (ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (dbCon != null) {
				try {
					dbCon.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
				
	}
	
	private static void printOutUmsatzProdukt(String pid) {	
		Statement st;
		try {
			st = dbCon.createStatement();
			
			ResultSet rs = st.executeQuery("SELECT orders.aid, agents.aname, SUM(dollars) as umsatz " + 
			"FROM orders JOIN agents ON orders.aid = agents.aid " + 
			"WHERE orders.pid = '" + pid + "' " +
			"GROUP BY orders.aid " + 
			"ORDER BY umsatz DESC;");
			
			while(rs.next()) {
				System.out.println(String.format("Agent: %s %s Umsatz: %d", rs.getString("aid"), rs.getString("aname"), rs.getInt("umsatz")));			
			}
			rs.close();
			st.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	private static void initiateConnection() throws ClassNotFoundException, SQLException {
		dbCon = DriverManager.getConnection("jdbc:mysql://Datenbank-PC/cap-datenbank", "dbuser", "daten2");		
	}

	private static String readIn() {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		try {			
			return br.readLine().trim();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		return "";
	}
	
}
