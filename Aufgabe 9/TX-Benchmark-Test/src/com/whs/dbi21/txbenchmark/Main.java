package com.whs.dbi21.txbenchmark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * Benchmark-Test 2
 * 
 * Diese Anwendung wurde fuer ein Praktikum in DBI entworfen und testet ein MySQL-DBMS. Dabei werden
 * realitaetsnahe Anwendungsfaelle getestet. 
 * In diesem Beispiel soll ein Banksystem simuliert werden, in dem folgende drei Transaktionen 
 * durchgefuehrt werden:
 * 		- Geldeinzahlung
 * 		- Kontostandsabfrage
 * 		- Analyseabfrage, die eine History-Tabelle durchforstet
 * 
 * Anmerkung: Die Geldeinzahlungstransaktion belastet mehrere Tabellen.
 * 
 * Es wird zufaellig eine dieser Transaktion ausgewaehlt, die dann abgesetzt wird. Es folgt eine 50 ms Pause und 
 * anschliessend wird die naechste zufaellig ausgewaehlte Aktion ausgefuehrt.
 * Ausgefuehrt wird der beschreibene Ablauf von n-vielen LoadDriver, die parallel laufen. Die Anzahl kann
 * beim Start des Programms festgelegt werden.
 * 
 * Der Benchmark-Test hat den folgenden zeitliche Rahmen:
 * Zuerst werden die Transaktionen waehrend der Aufwaermphase 4 Minuten lang in einer Schleife abgeschickt, 
 * ohne, dass sie gezaehlt werden. Nach 4 Minuten beginnt die eigentlich Messphase. Die Transaktionen werden
 * weiterhin durchgehend abgeschickt, bis die Messphase von 5 Minuten endet. Zum Schluss wird eine 1 minuetige 
 * Auslaufphase durchgefuehrt.
 * 
 *  Waehrend der Messphase wird die Anzahl der durchgefuehrten Transaktionen von jedem LoadDriver protokolliert.
 *  Sobald die 10 Minuten vorueber sind, werden die Messdaten von jedem LoadDriver addiert, analysiert und 
 *  ausgegeben.
 * 
 *  Weiterfuehrende Informationen lassen sich der beiliegenden Dokumentation entnehmen.
 * 
 * @author Timo Knufmann, Johannes Nowack, Andre Schluess
 *
 */
public class Main {
	
	// Beinhaltet alle erzeugten LoadDriver 
	private static ArrayList<LoadDriver> loadDriverList;
	
	private static Scanner scanner;
	
	/**
	 * Hauptsteuerung der Anwendung: 
	 * In der main-Methode werden Parameter mittels des Standardinputstreams eingelesen. 
	 * Zu den Parametern zaehlen sowohl die verschiedenen Zeitspannen fuer die drei Phasen Aufwaermphase, 
	 * Messphase und Auslaufphase eingelesen, als auch die Anzahl der zu erzeugenden LoadDriver.
	 * 
	 * Anschließend werden die LoadDriver erzeugt und gestartet.	 
	 * Waehrend des Benchmarks findet in der main-Methode die Steuerung des Tests statt, sodass diese Methode
	 * den einzelnen LoadDrivern mitteilt, wann sie den Messvorgang starten bzw. stoppen sollen. 
	 *
	 * @param args
	 */
	public static void main(String[] args) {	
		scanner = new Scanner(System.in);
		
		System.out.println("Benchmark-Test 2:\n\n");
		
		// Einlesen der Werte fuer die Zeitspannen der drei Phasen und Einlesen
		// der Anzahl LoadDriver die erzeugt werden sollen.
		System.out.print("Anzahl LoadDriver: ");
		int countLoadDriver = readNumberInInt();
		
		System.out.print("\nDauer Einschwingphase [in Sekunden, default: 240]: ");
		int durationFirstPhase = readNumberInInt();
		durationFirstPhase = durationFirstPhase > 0 ? durationFirstPhase : 240;
		
		System.out.print("\nDauer Messphase [in Sekunden, default: 300]: ");
		int durationSecondPhase = readNumberInInt();
		durationSecondPhase = durationSecondPhase > 0 ? durationSecondPhase : 300;
		
		System.out.print("\nDauer Ausschwingphase [in Sekunden, default: 60]: ");
		int durationThirdPhase = readNumberInInt();
		durationThirdPhase = durationThirdPhase > 0 ? durationThirdPhase : 60;		
		
		// Bereinigung der Datenbank. In diesem Fall wird lediglich die History-Tabelle geleert
		try {
			System.out.println("\nDatenbank säubern...");
			cleanDatabase();
			System.out.println("Datenbank gesäubert!");
		} catch (SQLException e1) {
			e1.printStackTrace();
			System.out.println("Datenbank säubern fehlgeschlagen!\n Terminierung der Anwendung!");
			System.exit(-1);
		}		
		
		System.out.println("------------------------------------------");
		System.out.println("Benchmark-Test gestartet!\n");
		try {
			loadDriverList = new ArrayList<LoadDriver>();
			
			// Erzeugung der LoadDriver
			for (int i = 0; i < countLoadDriver; i++) {
				loadDriverList.add(new LoadDriver(i));	
			}
			// Starten der LoadDriver
			for (LoadDriver ld : loadDriverList) 
				ld.start();		
			
			// Warten bis die erste Phase (Auswaermphase) abgeschlossen ist
			Thread.sleep(durationFirstPhase * 1000);
			
			// Eintritt in die zweite Phase (Messphase) mit Starten der Messungen
			for (LoadDriver ld : loadDriverList)
				ld.startMeasuring();
			
			// Warten bis zweite Phase abgeschlossen
			Thread.sleep(durationSecondPhase * 1000);
			
			// Eintritt in die dritte Phase (Auslaufphase). Stoppen der Messung
			for (LoadDriver ld : loadDriverList)
				ld.stopMeasuring();
			
			Thread.sleep(durationThirdPhase * 1000);
			
			// Nach Fertigstellung des Tests, werden die LoadDriver beendet, sodass Datenbankverbindungen
			// sauber geschlossen werden koennen
			for (LoadDriver ld : loadDriverList)
				ld.stopLoadDriver();
			
			System.out.println("------------------------------------------");			
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (SQLException e) {			
			e.printStackTrace();
			System.out.println("Error bei Verbindung zur DB!\nTerminierung der Anwendung!");
			System.exit(-1);
		}
		
		System.out.println("Benchmark-Test abgeschlossen!\n\nErgebnisse der einzelnen LoadDriver:\n");
		
		int totalCountAllLoadDriver = 0;
		for (LoadDriver ld : loadDriverList) {
			// Ausgabe der Messergebnisse jedes einzelnen LoadDrivers
			System.out.println(ld.getLoadDriverId() + ": | Balance-TXs: " + ld.getCountBalanceTx() +
					" | Inpayment-TXs: " + ld.getCountInpaymentTx() +
					" | Analyse-TXs: " + ld.getCountAnalyseTx() +
					" | Total count: " + ld.getCountTxTotal() +
					" | Error count: " + ld.getErrorCount());
			totalCountAllLoadDriver += ld.getCountTxTotal();
		}
		
		// Ausgabe aller Transaktionen gesamt und Ausgabe Transaktionen pro Sekunde
		System.out.println("\n\nGesamtergebnis: ");
		System.out.println("Transaktionen gesamt: " + totalCountAllLoadDriver);			
		System.out.println("Transaktionen pro Sekunde: " +	Math.round(totalCountAllLoadDriver / durationSecondPhase));
		
		System.out.println("------------------------------------------");
		scanner.close();
		System.exit(0);
	}
	
	/**
	 * Funktion zum Datenbank bereinigen. In diesem Fall wird lediglich die History-Tabelle geleert.
	 * 
	 * @throws SQLException
	 */
	private static void cleanDatabase() throws SQLException {
		Connection dbCon = initializeConnection();	
		
		// Leeren der History-Tabelle
		Statement st = dbCon.createStatement();
		st.executeUpdate("DELETE FROM history;");
		
		// Aktivieren des Transaktionslogs
		st.executeUpdate("SET GLOBAL general_log = 'ON';");
		
		// Isolationslevel auf Serializable setzen
		st.executeUpdate("SET GLOBAL TRANSACTION ISOLATION LEVEL SERIALIZABLE;");
		
		st.close();
		dbCon.close();
	}

	/**
	 * Funktion zum Einlesen eines Integers aus dem Standardinputstream.
	 * 
	 * @return Gibt den eingelesen Integer zurück, bei Fehlerfall -1.
	 */
	private static int readNumberInInt() {
        String s = scanner.next();
        try {
            int i = Integer.parseInt(s);
            return i;
        } catch (NumberFormatException e) {
            return -1;
        }        
    }
	
	/**
	 * Stellt die Verbindung zu einer Datenbank her. Die Daten müssen in der DbConnectionInfo-Klasse 
	 * eingetragen werden
	 * 
	 * @return Gibt ein aktives Connection-Objekt zurück.
	 * @throws SQLException
	 */
	private static Connection initializeConnection() throws SQLException {
		return DriverManager.getConnection(DbConnectionInfo.JDBCSTRING, DbConnectionInfo.DBUSER, DbConnectionInfo.DBPASSWORD);
	}
}
