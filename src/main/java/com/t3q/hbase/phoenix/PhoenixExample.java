package com.t3q.hbase.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * Phoenix 문법 : https://phoenix.apache.org/language/
 * table list > !tables
 * 
 * @author warmpark
 *
 */
public class PhoenixExample {

	public static void main(String[] args) {
		// Create variables
		Connection connection = null;
		Statement statement = null;
		ResultSet rs = null;
		PreparedStatement ps = null;

		try {
			// Connect to the database
			connection = DriverManager.getConnection("jdbc:phoenix:big03");

			// Create a JDBC statement
			statement = connection.createStatement();

			// Execute our statements
			statement.executeUpdate("create table IF NOT EXISTS javatest (mykey integer not null primary key, mycolumn varchar)");
			statement.executeUpdate("upsert into javatest values (1,'Hello')");
			statement.executeUpdate("upsert into javatest values (2,'Java Application')");
			connection.commit();

			// Query for table
			ps = connection.prepareStatement("select * from javatest");
			rs = ps.executeQuery();
			System.out.println("Table Values");
			while (rs.next()) {
				Integer myKey = rs.getInt(1);
				String myColumn = rs.getString(2);
				System.out.println("\tRow: " + myKey + " = " + myColumn);
			}
		} catch (Exception e) {
			try {
				connection.rollback();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			e.printStackTrace();
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (Exception e) {
				}
			}
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
				}
			}
			if (statement != null) {
				try {
					statement.close();
				} catch (Exception e) {
				}
			}
			if (connection != null) {
				try {
					connection.close();
				} catch (Exception e) {
				}
			}
		}
	}
}
