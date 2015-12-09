package com.whs.dbi21.txbenchmark;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class StatementsGenerator {

	public static int executeBalanceTx(int accId, Connection dbCon) throws SQLException {
		Statement st = dbCon.createStatement();
		
		ResultSet rs = st.executeQuery("SELECT balance FROM accounts WHERE accid = " + Integer.toString(accId) + ";");
		rs.next();
		
		int result = rs.getInt(0);
		rs.close();
		st.close();
		
		return result;
	}
	
	public static void executeInpaymentTx(int accId, int tellerId, int branchId, int delta, Connection dbCon) {
		
	}
	
	public static int executeAnalyseTx(int delta, Connection dbCon) {
		return 0;
	}
}
