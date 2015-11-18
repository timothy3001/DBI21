package com.whs.dbi21.benchmark;

public class Main {

	private static boolean initializeConnection() {
		return true;
	}
	
	public static void main(String[] args) {
		if (!initializeConnection()) { 
			System.out.println("Could not establish connection to database!");
			System.exit(-1);
		}			
	}
}
