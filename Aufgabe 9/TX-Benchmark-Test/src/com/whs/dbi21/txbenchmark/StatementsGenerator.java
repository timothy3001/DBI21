package com.whs.dbi21.txbenchmark;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class StatementsGenerator {

	/**
	 * Fuehrt eine SQL-Query durch und gibt als Wert den aktuellen Kontostand eines Bankaccounts zurueck.
	 * 
	 * @param accId Id des Bankkontos
	 * @param dbCon Ein aktives Connection-Objekt zu einer Datenbank
	 * @return Gibt den Kontostand zurueck
	 * @throws SQLException
	 */
	public static int executeBalanceTx(int accId, Connection dbCon) throws SQLException {
		Statement st = dbCon.createStatement();
		
		ResultSet rs = st.executeQuery("SELECT balance FROM accounts WHERE accid = " + accId + ";");
		rs.next();
		
		int result = rs.getInt(1);
		rs.close();
		st.close();
		
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
	public static int executeInpaymentTx(int accId, int tellerId, int branchId, int delta, Connection dbCon) throws SQLException {
		
		// Deaktivierung des AutoCommits. Dadurch soll sichergestellt werden, dass wenn ein Fehler auftritt
		// keine inkonsisten Datensaetze durch bereits aktualisierte Tabellen entstehen. Entweder die Transaktion
		// gelingt als Ganzes, oder sie schlaegt als Ganzes fehl. 
		
		boolean autocommit = dbCon.getAutoCommit();		
		dbCon.setAutoCommit(false);
		
		try {
			Statement st = dbCon.createStatement();		
			
			// Aktualisierung der Branches-Tabelle. Hier muss die Bilanz der Zweigstelle aufgrund der Einzahlung
			// aktualisiert werden.
			st.executeUpdate("UPDATE branches SET balance = balance + " + delta + " WHERE branchid = " + branchId + ";");
			
			// Aktualisierung der Tellers-Tabelle. Hier muss die Bilanz des Geldautomatens aufgrund der Einzahlung
			// aktualisiert werden.
			st.executeUpdate("UPDATE tellers SET balance = balance + " + delta + " WHERE tellerid = " + tellerId + ";");
			
			// Eigentliche Aktualisierung des Kontostands eines Kontos.
			st.executeUpdate("UPDATE accounts SET balance = balance + " + delta + " WHERE accid = " + accId + ";");
			
			// Abrufen des aktuellen Kontostandes
			int currentBalance = executeBalanceTx(accId, dbCon);
			
			// Einfuegen einer Zeile in die History-Tabelle, die Informationen ueber die aktuelle 
			//Transaktion enthaelt.
			st.executeUpdate("INSERT INTO history (accid, tellerid, delta, branchid, accbalance, cmmnt) VALUES(" + accId + ", " + tellerId + ", " + delta + ", " + branchId + ", " + currentBalance + ", 'INPAYMENT');");
			
			dbCon.commit();	
			
			st.close();
			
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
	public static int executeAnalyseTx(int delta, Connection dbCon) throws SQLException {
		Statement st = dbCon.createStatement();
		
		ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM history WHERE delta = " + delta + ";");
		rs.next();		
		int result = rs.getInt(1);
		
		st.close();
		return result;
	}
}
