package com.whs.dbi21.benchmark;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

public class Statements {
	static void createdb(int pn, Connection pconnection){
		
		PreparedStatement stmt=null;
		String name=createString(20);
		String addresslong=createString(72);
		String addressshort=createString(68);
		
		try {
			
			pconnection.setAutoCommit(false);
			stmt = pconnection.prepareStatement("INSERT INTO branches (branchid, balance, branchname, address) " + "VALUES (?,?,?,?)");
			
			for (int i=0;i<pn;i++){
				stmt.setInt(1, i+1);
				stmt.setInt(2, 0);
				stmt.setString(3, name);
				stmt.setString(4, addresslong);
				stmt.executeUpdate();
			}
			pconnection.commit();
			
			stmt.close();
			//stmt.executeUpdate("INSERT INTO branches (branchid, balance, branchname, address) " + "VALUES ("+(i+1)+",0,'"+name+"','"+addresslong+"')");
			
			
			
			stmt = pconnection.prepareStatement("INSERT INTO accounts (accid, balance, branchid, name, address)" + "VALUES (?,?,?,?,?)");
			
			for (int i=0;i<pn*100000;i++){
				stmt.setInt(1, i+1);
				stmt.setInt(2, 0);
				stmt.setInt(3, ((int)(Math.random()*pn)+1));
				stmt.setString(4, name);
				stmt.setString(5, addressshort);
				stmt.executeUpdate();
			}
			pconnection.commit();
			
			stmt.close();
			
			
			stmt = pconnection.prepareStatement("INSERT INTO tellers (tellerid, balance, branchid, tellername, address)" + "VALUES (?,?,?,?,?)");
			
			for (int i=0;i<pn*10;i++){
				stmt.setInt(1, i+1);
				stmt.setInt(2, 0);
				stmt.setInt(3, ((int)(Math.random()*pn)+1));
				stmt.setString(4, name);
				stmt.setString(5, addressshort);
				stmt.executeUpdate();
			}
			pconnection.commit();
			
			stmt.close();
				//stmt.executeUpdate("INSERT INTO tellers (tellerid, balance, branchid, tellername, address)" + "VALUES ("+(i+1)+",0,"+((int)(Math.random()*pn)+1)+",'"+name+"','"+addressshort+"')");

			
			
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
	}
	
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
