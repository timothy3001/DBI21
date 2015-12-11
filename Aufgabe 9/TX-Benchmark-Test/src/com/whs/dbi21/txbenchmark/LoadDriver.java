package com.whs.dbi21.txbenchmark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LoadDriver extends Thread {
	
	//TODO ExceptionHandling gescheit machen
	
	private boolean isStopped = false;
	private boolean isMeasuring = false;
	
	private int countBalanceTx = 0;
	private int countInpaymentTx = 0;
	private int countAnalyseTx = 0;
	private int errorCount = 0;
	
	private int loadDriverId;

	private Connection dbCon;
	
	private void initializeConnection() throws SQLException {
		dbCon = DriverManager.getConnection(DbConnectionInfo.JDBCSTRING, DbConnectionInfo.DBUSER, DbConnectionInfo.DBPASSWORD);
	}
	
	public LoadDriver(int loadDriverId) throws SQLException{
		this.loadDriverId = loadDriverId;
		
		log("Initializing");
		try {
			initializeConnection();
			log("Connection to database established");		
		} catch (SQLException e) {
			e.printStackTrace();
			log("\nCould not establish connection to database!");			
			throw e;			
		}
	}
	
	public void run(){
		log("Firing statements");
		while (!isStopped){
			try {
				callStatement();
				Thread.sleep(50);
			} catch (SQLException e) {
				errorCount++;
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}	
		}
		log("LoadDriver stopped!");
		try {
			dbCon.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void callStatement() throws SQLException{
		int random = (int) (Math.random() * 100);
		
		if (random < 35){
			StatementsGenerator.executeBalanceTx((int)(Math.random() * 100000 * 100) + 1, dbCon);
			if (isMeasuring)
				countBalanceTx++;
		} else if (random < (50 + 35)) {
			StatementsGenerator.executeInpaymentTx((int)(Math.random()*100000 * 100) + 1, 
					(int)(Math.random() * 10 * 100) + 1, 
					(int)(Math.random() * 1 * 100) + 1, 
					(int)(Math.random() * 10000) + 1, 
					dbCon);
			if (isMeasuring)
				countInpaymentTx++;
		} else {
			StatementsGenerator.executeAnalyseTx((int) (Math.random() * 100000 * 100) + 1, dbCon);
			if (isMeasuring)
				countAnalyseTx++;
		}
	}
	
	public void startMeasuring(){
		isMeasuring = true;
		log("Start measuring");
	}
	
	public void stopMeasuring(){
		isMeasuring = false;
		log("Stop measuring");
		// System.out.println(loadDriverId + ": Messung: BalanceTx:" + countBalanceTx + " InpaymentTx:" + countInpaymentTx + " AnalyseTx:" + countAnalyseTx);
	}
	
	public int getCountTxTotal(){
		return countAnalyseTx + countBalanceTx + countInpaymentTx;
	}
	
	public void stopLoadDriver() {
		isStopped = true;
	}
	
	public int getCountBalanceTx() {
		return countBalanceTx;
	}

	public int getCountInpaymentTx() {
		return countInpaymentTx;
	}

	public int getCountAnalyseTx() {
		return countAnalyseTx;
	}
	
	public int getErrorCount() {
		return errorCount;
	}
	
	public int getLoadDriverId() {
		return loadDriverId;
	}
	
	private SimpleDateFormat sdf = null;
	
	private void log(String msg) {
		if (sdf == null)
			sdf = new SimpleDateFormat("HH:mm:ss");
		System.out.println("[" + sdf.format(new Date()) + "] " + loadDriverId + ": " + msg);
	}
}
