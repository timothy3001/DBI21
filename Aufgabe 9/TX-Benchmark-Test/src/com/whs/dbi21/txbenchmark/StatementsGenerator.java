package com.whs.dbi21.txbenchmark;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class StatementsGenerator {

	private Connection dbCon;
	private PreparedStatement prepStatementBalanceQuery;
	private PreparedStatement prepStatementInpaymentBranches;
	private PreparedStatement prepStatementInpaymentTellers;
	private PreparedStatement prepStatementInpaymentAccounts;
	private PreparedStatement prepStatementInpaymentHistory;
	private PreparedStatement prepStatementAnalyseQuery;
	
	public StatementsGenerator(Connection dbCon) throws SQLException {
		this.dbCon = dbCon;
		
		prepStatementBalanceQuery = dbCon.prepareStatement("SELECT balance FROM accounts WHERE accid = ?;");
		
		prepStatementInpaymentBranches = dbCon.prepareStatement("UPDATE branches SET balance = balance + ? WHERE branchid = ?;");
		prepStatementInpaymentTellers = dbCon.prepareStatement("UPDATE tellers SET balance = balance + ? WHERE tellerid = ?;");
		prepStatementInpaymentAccounts = dbCon.prepareStatement("UPDATE accounts SET balance = balance + ? WHERE accid = ?;");
		prepStatementInpaymentHistory = dbCon.prepareStatement("INSERT INTO history (accid, tellerid, delta, branchid, accbalance, cmmnt) VALUES(?, ?, ?, ?, ?, 'INPAYMENT');");
		
		prepStatementAnalyseQuery = dbCon.prepareStatement("SELECT COUNT(*) FROM history WHERE delta = ?;");
	}
	
	/**
	 * Fuehrt eine SQL-Query durch und gibt als Wert den aktuellen Kontostand eines Bankaccounts zurueck.
	 * 
	 * @param accId Id des Bankkontos
	 * @param dbCon Ein aktives Connection-Objekt zu einer Datenbank
	 * @return Gibt den Kontostand zurueck
	 * @throws SQLException
	 */
	public int executeBalanceTx(int accId) throws SQLException {
		prepStatementBalanceQuery.setInt(1, accId);
		ResultSet rs = prepStatementBalanceQuery.executeQuery();
		
		rs.next();		
		int result = rs.getInt(1);
		rs.close();		
		return result;
	}
	
	/**
	 * Fuehrt eine SQL-Transaktion durch, die bei einer Geldeinzahlung auf ein Konto verwendet wird. Dabei
	 * muessen neben der Accounts-Tabelle auch die Branches- und Tellers-Tabelle aktualisiert werden, da sich 
	 * die Balance durch eine Einzahlung auch bei ihnen aendert. Des Weiteren muss die Transaktion in der
	 * History-Tabelle festgehalten werden.
	 * 
	 * Als Ergebnis gibt die Funktion den neuen Kontostand zurueck.
	 * 
	 * @param accId Id des Kontos auf das eingezahlt werden soll
	 * @param tellerId Id des Geldautomatens an dem eingezahlt werden soll
	 * @param branchId Id der Zweigstelle, zu der der Geldautomat gehoert
	 * @param delta Geldsumme der Einzahlung
	 * @param dbCon Ein aktives Connection-Objekt zu einer Datenbank
	 * @return Gibt den neuen Kontostand zurueck
	 * @throws SQLException
	 */
	public int executeInpaymentTx(int accId, int tellerId, int branchId, int delta) throws SQLException {
		
		// Deaktivierung des AutoCommits. Dadurch soll sichergestellt werden, dass wenn ein Fehler auftritt
		// keine inkonsisten Datensaetze durch bereits aktualisierte Tabellen entstehen. Entweder die Transaktion
		// gelingt als Ganzes, oder sie schlaegt als Ganzes fehl. 
		
		boolean autocommit = dbCon.getAutoCommit();		
		dbCon.setAutoCommit(false);
		
		try {			
			// Aktualisierung der Branches-Tabelle. Hier muss die Bilanz der Zweigstelle aufgrund der Einzahlung
			// aktualisiert werden.
			prepStatementInpaymentBranches.setInt(1, delta);
			prepStatementInpaymentBranches.setInt(2, branchId);
			prepStatementInpaymentBranches.executeUpdate();
			
			// Aktualisierung der Tellers-Tabelle. Hier muss die Bilanz des Geldautomatens aufgrund der Einzahlung
			// aktualisiert werden.
			prepStatementInpaymentTellers.setInt(1, delta);
			prepStatementInpaymentTellers.setInt(2, tellerId);
			prepStatementInpaymentTellers.executeUpdate();
			
			// Eigentliche Aktualisierung des Kontostands eines Kontos.
			prepStatementInpaymentAccounts.setInt(1, delta);
			prepStatementInpaymentAccounts.setInt(2, accId);	
			prepStatementInpaymentAccounts.executeUpdate();
			
			// Abrufen des aktuellen Kontostandes
			int currentBalance = executeBalanceTx(accId);
			
			// Einfuegen einer Zeile in die History-Tabelle, die Informationen ueber die aktuelle 
			//Transaktion enthaelt.
			prepStatementInpaymentHistory.setInt(1, accId);
			prepStatementInpaymentHistory.setInt(2, tellerId);
			prepStatementInpaymentHistory.setInt(3, delta);
			prepStatementInpaymentHistory.setInt(4, branchId);
			prepStatementInpaymentHistory.setInt(5, currentBalance);
			prepStatementInpaymentHistory.executeUpdate();
			
			dbCon.commit();	
			
			return currentBalance;
		} finally {
			// Autocommit wieder auf alten Stand setzen, um weitere Transaktionen nicht zu beeinflussen.
			dbCon.setAutoCommit(autocommit);
		}
	}
	
	/**
	 * Fuehrt eine SQL-Transaktion durch, bei der Berechnungen in der History-Tabelle durchgefuehrt weden.
	 * Die Funktion gibt die Anzahl der Transaktionen zurueck, bei der eine bestimmte Summe Geld
	 * eingezahlt wurde. 
	 * 
	 * @param delta Geldsumme, fuer die die Anzahl bestimmt werden soll
	 * @param dbCon Ein aktives Connection-Objekt zu einer Datenbank
	 * @return Anzahl der Transaktionen
	 * @throws SQLException
	 */
	public int executeAnalyseTx(int delta) throws SQLException {
		prepStatementAnalyseQuery.setInt(1, delta);
		ResultSet rs = prepStatementAnalyseQuery.executeQuery();
		
		rs.next();						
		return rs.getInt(1);
	}
	
	public void closeStatements() throws SQLException {
		prepStatementAnalyseQuery.close();
		prepStatementBalanceQuery.close();
		prepStatementInpaymentAccounts.close();
		prepStatementInpaymentBranches.close();
		prepStatementInpaymentHistory.close();
		prepStatementInpaymentTellers.close();
	}
}
