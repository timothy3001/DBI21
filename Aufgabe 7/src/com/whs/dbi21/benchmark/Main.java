package com.whs.dbi21.benchmark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.Scanner;

import com.whs.dbi21.benchmark.DbConnectionInfo;

/**
 * Benchmark-Test für MySQL
 * 
 * In diesem Benchmark-Test werden abhängig von n viele INSERT-Statements zur Datenbank geschickt. Beim Aufruf
 * der main-Methode hat der User die Möglichkeit einen Wert für n einzutragen. Üblicherweise werden Werte von 1-50
 * für n verwendet, um die Datenbank zu testen. 
 * 
 * Sobald der Test durchgelaufen ist, wird die Dauer in Sekunden zurückgegeben. 
 * 
 * Des Weiteren werden die Tabellen zur Vorbereitung des Tests beim Aufruf der main-Methode geleert. 
 * 
 * @author Johannes Nowack, André Schlüß, Timo Knufmann
 *
 */
public class Main {

	private static Connection dbCon;
	
	/**
	 * Stellt die Verbindung zu einem DBMS her. Wenn eine Verbindung erfolgreich hergestellt werden konnte,
	 * wird true zurückgegeben. 
	 * 
	 * @return Gibt false zurück, falls es einen Fehler beim Herstellen der Verbindung zu einem DBMS gab
	 */
	private static boolean initializeConnection() {
		try {
			dbCon = DriverManager.getConnection(DbConnectionInfo.JDBCSTRING, DbConnectionInfo.DBUSER, DbConnectionInfo.DBPASSWORD);
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}
	
	/**
	 * Start des Programms. Zu erst wird versucht eine Verbindung zu einem DBMS herzustellen. Anschließend
	 * werden die Tabellen geleert. Daraufhin wird der eigentliche Benchmark-Test ausgeführt und am Ende
	 * wird die benötigte Dauer in Sekunden zurückgegeben.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		// Herstellen der Verbindung zur Datenbank		
		if (!initializeConnection()) { 
			System.out.println("Could not establish connection to database!");
			System.exit(-1);
		}			
		System.out.println("Connection to database established!");		
		
		// Aufräumen der Tabellen
		cleanDatabase();				
		System.out.println("Database cleaned!");
		
		// Einlesen von n. Falls Wert nicht gültig, wird der default 10 verwendet
		System.out.print("Please enter a number for n: ");
		int n = readNumberInInt();
		if (n < 1) {
			System.out.println("No valid number for n was entered, using default: 10");
			n = 10;
		}		
		
		// Starten des Benchmark-Tests und festhalten der Zeitpunkte
		System.out.println("Start benchmark test!");
		Date testStart = new Date();
		Statements.createdb(n, dbCon);	
		Date testFinish = new Date();
		
		// Ausgabe der benötigten Zeit in Sekunden
		long timeUsed = testFinish.getTime() - testStart.getTime();
		System.out.println("Time used in seconds = " + (double)timeUsed / 1000);		
	}

	/**
	 * Bereinigt die Benchmark-Test Datenbank.
	 * 
	 * @return Gibt true zurück, wenn das Bereinigen erfolgreich ausgeführt werden konnte.
	 */
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
	
	/**
	 * Liest eine Integervariablen aus dem STDIN ein und gibt diese zurück. Falls es sich um keine gültige Zahl handelt,
	 * wird -1 zurückgegeben
	 * 
	 * @return Eingelesene Integerzahl
	 */
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
