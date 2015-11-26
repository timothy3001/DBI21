package com.whs.dbi21.benchmark;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

/**
 * In dieser Klasse wird der eigentliche Benchmark-Test durchgef�hrt. Daf�r wird eine bereits bestehende Verbindung
 * zu einer Datenbank ben�tigt.
 * 
 * @author Andr� Schl��, Johannes Nowack, Timo Knufmann
 *
 */
public class Statements {
	
	/**
	 * F�hrt den eigentlichen Benchmark-Test durch. Es werden n Datens�tze in der "branches" Tabelle angelegt.
	 * Des Weiteren werden n * 100000 Datens�tze in der "accounts" Tabelle angelegt und n * 10 Datens�tze in der
	 * "tellers" Datenbank.
	 * 
	 * @param pn Faktor f�r die Anzahl der anzulegenden Daten
	 * @param pconnection Datenbankverbindung zur Datenbank, auf der der Test durchgef�hrt werden soll
	 */
	static void createdb(int pn, Connection pconnection){
		
		PreparedStatement stmt=null;
		
		// Erzeugung zuf�lliger Strings, die im Benchmark als "Dummies" verwendet werden
		String name = createString(20);
		String addresslong = createString(72);
		String addressshort = createString(68);
		
		try {
			// Deaktivierung AutoCommit (Optimierung)
			pconnection.setAutoCommit(false);
			
			// Verwendung von Prepared Statements (ebenfalls Optimierung)
			stmt = pconnection.prepareStatement("INSERT INTO branches (branchid, balance, branchname, address) " + "VALUES (?,?,?,?)");
			
			for (int i=0;i<pn;i++){
				// Einf�gen von Datens�tzen in die Tabelle "branches" mithilfe der Prepared Statements
				stmt.setInt(1, i+1);
				stmt.setInt(2, 0);
				stmt.setString(3, name);
				stmt.setString(4, addresslong);
				stmt.addBatch();
			}
			
			// Ausf�hrung des ersten Stapels, f�r die "branches" Tabelle
			stmt.executeBatch();			
			stmt.close();		
			
			// INSERT-Statements f�r die Tabelle "accounts" vorbereiten und ausf�hren
			stmt = pconnection.prepareStatement("INSERT INTO accounts (accid, balance, branchid, name, address)" + "VALUES (?,?,?,?,?)");
			
			for (int i=0;i<pn*100000;i++){
				stmt.setInt(1, i+1);
				stmt.setInt(2, 0);
				stmt.setInt(3, ((int)(Math.random()*pn)+1));
				stmt.setString(4, name);
				stmt.setString(5, addressshort);
				stmt.addBatch();
			}
			
			stmt.executeBatch();
			
			stmt.close();
			
			// INSERT-Statements f�r die "tellers" Tabelle vorbereiten und ausf�hren
			stmt = pconnection.prepareStatement("INSERT INTO tellers (tellerid, balance, branchid, tellername, address)" + "VALUES (?,?,?,?,?)");
			
			for (int i=0;i<pn*10;i++){
				stmt.setInt(1, i+1);
				stmt.setInt(2, 0);
				stmt.setInt(3, ((int)(Math.random()*pn)+1));
				stmt.setString(4, name);
				stmt.setString(5, addressshort);
				//stmt.executeUpdate();
				stmt.addBatch();
			}
			
			stmt.executeBatch();
			
			// Commit in der Datenbank aufrufen
			pconnection.commit();
			
			stmt.close();
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
	}
	
	/**
	 * Erzeugt einen zuf�lligen String der L�nge azchar
	 * 
	 * @param azchar L�nge des zuf�lligen Strings
	 * @return Zuf�lliger String
	 */
	static String createString(int azchar){
		String rString="";
		Random rnd=new Random();
		
		String alphabet="abcdef";
		
		for (int i=0;i<azchar;i++){
			rString=rString+alphabet.charAt(rnd.nextInt(alphabet.length()));
		}
		
		return rString;
	}
}
