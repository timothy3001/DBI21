package com.whs.dbi21.txbenchmark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class LoadDriver extends Thread {
	
	//TODO ExceptionHandling gescheit machen
	
	private boolean misst;
	private int az;
	private int nr;
	private static Connection dbCon;
	
	public static void main(String[] args){
		
	}
	
	private static void initializeConnection() throws SQLException {
		dbCon = DriverManager.getConnection(DbConnectionInfo.JDBCSTRING, DbConnectionInfo.DBUSER, DbConnectionInfo.DBPASSWORD);
	}
	
	public LoadDriver(int pnr){
		nr=pnr;
		az=0;
		
		try {
			initializeConnection();
			System.out.println(nr+": Connection to database established!");		
		} catch (SQLException e) {
			e.printStackTrace();
			System.out.println(nr+": \nCould not establish connection to database!");			
			System.exit(-1);
		}
	}
	
	public void run(){
		while (true){
			try {
				callStatement();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			if (misst)
				az++;
			
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public void callStatement() throws SQLException{
		int zz=(int) (Math.random()*100);
		
		if (zz<35){
			StatementsGenerator.executeBalanceTx((int) (Math.random()*100000*100), dbCon);
		}else if (zz<(50+35)){
			StatementsGenerator.executeInpaymentTx((int) (Math.random()*100000*100),(int) (Math.random()*10*100),(int) (Math.random()*1*100),(int) (Math.random()*10000)+1, dbCon);
		}else{
			StatementsGenerator.executeAnalyseTx((int) (Math.random()*100000*100), dbCon);
		}
	}
	
	public void starteMessung(){
		misst=true;
		System.out.println(nr+": starte");
	}
	
	public void stoppeMessung(){
		misst=false;
		System.out.println(nr+": Messung: "+az);
	}
	
	public int getAz(){
		return az;
	}
}
