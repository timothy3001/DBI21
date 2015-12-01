package com.whs.dbi21.benchmark;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
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
		Statement stmt2;
		
		// Erzeugung zuf�lliger Strings, die im Benchmark als "Dummies" verwendet werden
		String name = createString(20);
		String addresslong = createString(72);
		String addressshort = createString(68);
		//System.out.println(prepareInsertString("blabla",2,5));
		//System.exit(0);
		
		String insertB = prepareInsertString("INSERT INTO branches (branchid, balance, branchname, address) VALUES",4,pn);
		String insertA = prepareInsertString("INSERT INTO accounts (accid, balance, branchid, name, address) VALUES",5,50000);
		String insertT = prepareInsertString("INSERT INTO tellers (tellerid, balance, branchid, tellername, address) VALUES",5,pn*10);
		
		
		try {
			// Deaktivierung AutoCommit (Optimierung)
			pconnection.setAutoCommit(false);
			
			stmt2 = pconnection.createStatement();
			stmt2.execute("SET FOREIGN_KEY_CHECKS=1");
			stmt2.close();
			pconnection.commit();
			
			// Verwendung von Prepared Statements (ebenfalls Optimierung)
			stmt = pconnection.prepareStatement(insertB);
			//System.out.println(insertB);
			
			for (int i=0;i<pn;i++){
				// Einf�gen von Datens�tzen in die Tabelle "branches" mithilfe der Prepared Statements
				stmt.setInt(i*4+1, i+1);
				stmt.setInt(i*4+2, 0);
				stmt.setString(i*4+3, name);
				stmt.setString(i*4+4, addresslong);
			}
			stmt.executeUpdate();
			//stmt.addBatch();
			
			// Ausf�hrung des ersten Stapels, f�r die "branches" Tabelle
			//stmt.executeBatch();			
			stmt.close();		
			
			// INSERT-Statements f�r die Tabelle "accounts" vorbereiten und ausf�hren
			stmt = pconnection.prepareStatement(insertA);

			for (int j=0;j<pn*2;j++){
				for (int i=0;i<50000;i++){
					stmt.setInt(i*5+1, i+1+j*50000);
					stmt.setInt(i*5+2, 0);
					stmt.setInt(i*5+3, ((int)(Math.random()*pn)+1));
					stmt.setString(i*5+4, name);
					stmt.setString(i*5+5, addressshort);
				}
				//stmt.addBatch();
				stmt.executeUpdate();
			}
			//stmt.executeBatch();
			
			
			stmt.close();
			
			// INSERT-Statements f�r die "tellers" Tabelle vorbereiten und ausf�hren
			stmt = pconnection.prepareStatement(insertT);
			
			for (int i=0;i<pn*10;i++){
				stmt.setInt(i*5+1, i+1);
				stmt.setInt(i*5+2, 0);
				stmt.setInt(i*5+3, ((int)(Math.random()*pn)+1));
				stmt.setString(i*5+4, name);
				stmt.setString(i*5+5, addressshort);
			}
			stmt.executeUpdate();
			//stmt.addBatch();
			//stmt.executeBatch();
			stmt.close();
			
			// Commit in der Datenbank aufrufen
			pconnection.commit();
			
			stmt2 = pconnection.createStatement();
			stmt2.execute("SET FOREIGN_KEY_CHECKS=0");
			stmt2.close();
			pconnection.commit();
			
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
	
	static String prepareInsertString(String head, int azparam, int azwdh){
		StringBuilder prepInsert=new StringBuilder();
		
		prepInsert.append(head);
		
		for (int i1=0;i1<azwdh;i1++){
			prepInsert.append("(");
			for (int i2=0;i2<azparam;i2++){
				prepInsert.append("?");
				if (i2<azparam-1){
					prepInsert.append(",");
				}
			}
			prepInsert.append(")");
			if (i1<azwdh-1){
				prepInsert.append(",");
			}
		}
		
		prepInsert.append(";");
		return prepInsert.toString();
	}
}
