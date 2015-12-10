package com.whs.dbi21.txbenchmark;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class StatementsGenerator {

	public static int executeBalanceTx(int accId, Connection dbCon) throws SQLException {
		Statement st = dbCon.createStatement();
		
		ResultSet rs = st.executeQuery("SELECT balance FROM accounts WHERE accid = " + accId + ";");
		rs.next();
		
		int result = rs.getInt(0);
		rs.close();
		st.close();
		
		return result;
	}
	
	public static int executeInpaymentTx(int accId, int tellerId, int branchId, int delta, Connection dbCon) throws SQLException {
		Statement st = dbCon.createStatement();
		
		st.executeUpdate("UPDATE branches SET balance = balance + " + delta + " WHERE branchid = " + branchId + ";");
		st.executeUpdate("UPDATE tellers SET balance = balance + " + delta + " WHERE tellerid = " + tellerId + ";");
		st.executeUpdate("UPDATE accounts SET balance = balance + " + delta + " WHERE accid = " + accId + ";");
		
		int currentBalance = executeBalanceTx(accId, dbCon);
		st.executeUpdate("INSERT INTO history (accid, tellerid, delta, branchid, accbalance, cmmnt) VALUES(" + accId + ", " + tellerId + ", " + delta + ", " + branchId + ", " + currentBalance + ", 'INPAYMENT');");
		
		st.close();
		
		return currentBalance;
	}
	
	public static int executeAnalyseTx(int delta, Connection dbCon) throws SQLException {
		return 0;
	}
}
