package com.whs.dbi21.txbenchmark;

import java.util.ArrayList;

public class Main {
	
	//TODO Driver.jar Datei einbasteln
	
	static ArrayList<LoadDriver> drivers;
	
	public static void main(String[] args) {
		// Herstellen der Verbindung zur Datenbank	
		
		try {
			Messung(3);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		System.exit(0);
	}

	
	public static void Messung(int azDriver) throws InterruptedException{
		int messwert=0;
		drivers= new ArrayList<LoadDriver>();
		
		System.out.println("Start");
		
		for(int i=0;i<azDriver;i++){
			drivers.add(new LoadDriver(i));
			drivers.get(i).start();
		}
		
		Thread.sleep(2000);

		for(LoadDriver ld: drivers) {
			ld.starteMessung();
		}
		
		Thread.sleep(10000);		
		
		for(LoadDriver ld: drivers) {
			ld.stoppeMessung();
			messwert+=ld.getAz();
		}
		
		System.out.println("Messung: "+messwert);

		Thread.sleep(2000);

	}

}
