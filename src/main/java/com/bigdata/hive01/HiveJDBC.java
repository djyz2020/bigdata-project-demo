package com.bigdata.hive01;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class HiveJDBC {
	
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private static String url = "jdbc:hive2://master:10000/db_emp";
	private static String user = "root";
	private static String password = "root";
	
	public static void main(String[] args) {
		
		Connection conn = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		String sql = "";
		
		try {
			Class.forName(driverName);
			conn = DriverManager.getConnection(url, user, password);
			
			//show databases
			//sql = "show databases";
			sql = "select * from emp";
			pst = conn.prepareStatement(sql);
			rs = pst.executeQuery();
			while (rs.next()) {
				System.out.println(rs.getString(1) + "\t" + rs.getString(2) + "\t" + rs.getString(3));
			}
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		} finally {
			 try {
				 if(conn != null) {
					 conn.close();
				 }
				 if(pst != null){
					 //pst.close();
				 }
				 if(rs != null){
					 //rs.close();
				 }
			} catch (Exception e2) {
				e2.printStackTrace();
			}
		}

	}

}
