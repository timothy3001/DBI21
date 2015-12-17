package com.whs.dbi21.txbenchmark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class LoadDriver extends Thread {
		
	private boolean isStopped = false;
	private boolean isMeasuring = false;
	
	private StatementsGenerator statementsGenerator;
	
	private int countBalanceTx = 0;
	private int countInpaymentTx = 0;
	private int countAnalyseTx = 0;
	private int errorCount = 0;
	
	private int loadDriverId;

	private Connection dbCon;
	
	/**
	 * Herstellen der Datenbankverbindung
	 * @throws SQLException
	 */
	private void initializeConnection() throws SQLException {
		dbCon = DriverManager.getConnection(DbConnectionInfo.JDBCSTRING, DbConnectionInfo.DBUSER, DbConnectionInfo.DBPASSWORD);
	}
	
	/**
	 * Im Konstruktor wird die Datenbankverbindung initial erzeugt. Falls diese nicht hergestellt werden kann, 
	 * wird das Objekt nicht erzeugt, da es keine Tests ohne aktive DB-Verbindung ausfuehren kann.
	 * 
	 * Die Id des LoadDrivers kann beliebig gewaehlt werden und dient lediglich dazu, Log-Ausgaben zu 
	 * unterscheiden.
	 * 
	 * @param loadDriverId Id des LoadDrivers
	 * @throws SQLException
	 */
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
		
		statementsGenerator = new StatementsGenerator(dbCon);
	}
	
	/**
	 * Ueberladene run-Methode des Threads. Nach dem Start feuert diese ununterbrochen Transaktionen gegen
	 * die Datenbank. Sobald die Variable "isStopped" durch die Funktion "stopLoadDriver()" auf true gesetzt
	 * wird, beendet die Verarbeitung. Anschliessend wird die Datenbankverbindung geschlossen, sodass das 
	 * Objekt sauber zerstoert werden kann.
	 * 
	 * Bei einem Fehlerfall waehrend der Ausfuehrung einer Transaktion wird der ErrorCount erhoeht, der sich
	 * nachher ausgeben laesst.
	 */
	@Override
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
			statementsGenerator.closeStatements();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Zufaellig gewaehlter Aufruf einer der drei Benchmark-Transaktionen. Dabei wird die Verteilung, 
	 * die erreicht werden soll, ebenfalls beachtet.
	 * 
	 * @throws SQLException
	 */
	public void callStatement() throws SQLException{
		// Erzeugung einer zufaelligen Zahl, die zur Bestimmung der auszufuehrenden Transaktion unter 
		// Beruecksichtigung der gewaehlten Verteilung benoetigt wird.
		int random = (int) (Math.random() * 100);
		
		// Ausfuehren einer Transaktion abhaengig von der Zufallszahl
		if (random < 35){
			// Durchfuehrung einer Transaktionen
			statementsGenerator.executeBalanceTx((int)(Math.random() * 100000 * 100) + 1);
			// Falls das Messen in diesem LoadDriver aktiviert wurde, wird Zaehler fuer die jeweilige
			// Transaktion um einen erhoeht.
			if (isMeasuring)
				countBalanceTx++;
		} else if (random < (50 + 35)) {
			statementsGenerator.executeInpaymentTx((int)(Math.random() * 100000 * 100) + 1, 
					(int)(Math.random() * 10 * 100) + 1, 
					(int)(Math.random() * 1 * 100) + 1, 
					(int)(Math.random() * 10000) + 1);
			if (isMeasuring)
				countInpaymentTx++;
		} else {
			statementsGenerator.executeAnalyseTx((int) (Math.random() * 100000 * 100) + 1);
			if (isMeasuring)
				countAnalyseTx++;
		}
	}
	
	/**
	 * Aktivieren der Messung
	 */
	public void startMeasuring(){
		isMeasuring = true;
		log("Start measuring");
	}
	
	/**
	 * Deaktivieren der Messung
	 */
	public void stopMeasuring(){
		isMeasuring = false;
		log("Stop measuring");
	}
	
	/**
	 * Anhalten des LoadDrivers. Dabei wird auch die Datenbankverbindung beendet. Ein erneutes Starten ist 
	 * nicht moeglich.
	 */
	public void stopLoadDriver() {
		isStopped = true;		
	}
	
	/**
	 * Gibt die Anzahl aller durchgefuehrten Transaktionen waehrend des Messvorgangs zurueck.
	 * @return Anzahl Transaktionen
	 */
	public int getCountTxTotal(){
		return countAnalyseTx + countBalanceTx + countInpaymentTx;
	}
	
	/**
	 * Gibt die Anzahl der durchgefuehrten Transaktionen zurueck, bei denen der Kontostand abgefragt wurde.
	 * @return Anzahl Transaktionen
	 */
	public int getCountBalanceTx() {
		return countBalanceTx;
	}
	
	/**
	 * Gibt die Anzahl der durchgefuehrten Transaktionen zurueck, bei denen eine Einzahlung getaetigt wurde.
	 * @return Anzahl Transaktionen
	 */
	public int getCountInpaymentTx() {
		return countInpaymentTx;
	}
	
	/**
	 * Gibt die Anzahl der durchgefuehrten Transaktionen zurueck, denen die History-Tabelle analysiert wurde.
	 * @return Anzahl Transaktionen
	 */
	public int getCountAnalyseTx() {
		return countAnalyseTx;
	}
	
	/**
	 * Gibt die Anzahl der Transaktionen zurueck, bei denen eine SQLException aufgetreten ist.
	 * @return Anzahl fehlerhafte Transaktionen
	 */
	public int getErrorCount() {
		return errorCount;
	}
	
	/**
	 * Gibt die beim Erzeugen des Objektes festgelegte LoadDriver-Id zurueck.
	 * @return Id des LoadDrivers
	 */
	public int getLoadDriverId() {
		return loadDriverId;
	}
	
	private SimpleDateFormat sdf = null;
	
	/**
	 * Funktion zum Logging mit Zeitstempel und LoadDriver-Id
	 * 
	 * @param msg Nachricht, die geloggt werden soll
	 */
	private void log(String msg) {
		if (sdf == null)
			sdf = new SimpleDateFormat("HH:mm:ss.SSS");
		System.out.println("[" + sdf.format(new Date()) + "] " + loadDriverId + ": " + msg);
	}
}
