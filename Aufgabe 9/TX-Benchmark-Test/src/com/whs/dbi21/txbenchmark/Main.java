package com.whs.dbi21.txbenchmark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Scanner;

public class Main {
	
	private static ArrayList<LoadDriver> loadDriverList;
	private static Scanner scanner;
	
	public static void main(String[] args) {	
		scanner = new Scanner(System.in);
		
		System.out.println("Benchmark-Test 2:\n\n");
		
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
			
			for (int i = 0; i < countLoadDriver; i++) {
				loadDriverList.add(new LoadDriver(i));	
			}
			for (LoadDriver ld : loadDriverList) 
				ld.start();		
			
			Thread.sleep(durationFirstPhase * 1000);
			
			for (LoadDriver ld : loadDriverList)
				ld.startMeasuring();
			
			Thread.sleep(durationSecondPhase * 1000);
			
			for (LoadDriver ld : loadDriverList)
				ld.stopMeasuring();
			
			Thread.sleep(durationThirdPhase * 1000);
			
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
			System.out.println(ld.getLoadDriverId() + ": | Balance-TXs: " + ld.getCountBalanceTx() +
					" | Inpayment-TXs: " + ld.getCountInpaymentTx() +
					" | Analyse-TXs: " + ld.getCountAnalyseTx() +
					" | Total count: " + ld.getCountTxTotal() +
					" | Error count: " + ld.getErrorCount());
			totalCountAllLoadDriver += ld.getCountTxTotal();
		}
		
		System.out.println("\n\nGesamtergebnis: ");
		System.out.println("Transaktionen gesamt:" + totalCountAllLoadDriver);			
		System.out.println("Transaktionen pro Sekunde: " +	Math.round(totalCountAllLoadDriver / durationSecondPhase));
		
		System.out.println("------------------------------------------");
		scanner.close();
		System.exit(0);
	}

	private static void cleanDatabase() throws SQLException {
		Connection dbCon = initializeConnection();	
		
		Statement st = dbCon.createStatement();
		st.executeUpdate("DELETE FROM history;");
		
		st.close();
		dbCon.close();
	}

	private static int readNumberInInt() {
        String s = scanner.next();
        try {
            int i = Integer.parseInt(s);
            return i;
        } catch (NumberFormatException e) {
            return -1;
        }        
    }
	
	private static Connection initializeConnection() throws SQLException {
		return DriverManager.getConnection(DbConnectionInfo.JDBCSTRING, DbConnectionInfo.DBUSER, DbConnectionInfo.DBPASSWORD);
	}
}
