package com.t3q.hbase.client;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class CreateTable {
	public static void main(String[] args) throws IOException {
		Configuration config = HBaseConfiguration.create();

		// configuration -#1. 직접 설정
		config.set("hbase.master", "big01:600");
		config.set("hbase.zookeeper.quorum", "big01,big02,big03");
		config.set("hbase.zookeeper.property.clientPort", "2181");

		// configuration -#2. hbase-site.xml로 부터.
		// conf.addResource(new
		// Path("/JavaOneShot/IDE/64/workspace/BigData/src/main/resources/conf/hbase/hbase-site.xml"));

		Connection conn = ConnectionFactory.createConnection(config);
		Admin admin = conn.getAdmin();

		// create the table...
		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("people"));
		// ... with two column families
		tableDescriptor.addFamily(new HColumnDescriptor("name"));
		tableDescriptor.addFamily(new HColumnDescriptor("contactinfo"));
		admin.createTable(tableDescriptor);

		// define some people
		String[][] people = { { "1", "Marcel", "Haddad", "marcel@fabrikam.com" },
				{ "2", "Franklin", "Holtz", "franklin@contoso.com" }, { "3", "Dwayne", "McKee", "dwayne@fabrikam.com" },
				{ "4", "Rae", "Schroeder", "rae@contoso.com" }, { "5", "Rosalie", "burton", "rosalie@fabrikam.com" },
				{ "6", "Gabriela", "Ingram", "gabriela@contoso.com" } };

		Table table = conn.getTable(TableName.valueOf("people"));

		// Add each person to the table
		// 'name' column family for 'first' and 'last' qualifier
		// Use the 'contactinfo' column family for the email qualifier
		for (int i = 0; i < people.length; i++) {
			Put person = new Put(Bytes.toBytes(people[i][0]));
			person.addColumn(Bytes.toBytes("name"), Bytes.toBytes("first"), Bytes.toBytes(people[i][1]));
			person.addColumn(Bytes.toBytes("name"), Bytes.toBytes("last"), Bytes.toBytes(people[i][2]));
			person.addColumn(Bytes.toBytes("contactinfo"), Bytes.toBytes("email"), Bytes.toBytes(people[i][3]));
			table.put(person);
		}
		// flush commits and close the table
		table.close();
		
		// 조회
		// 테이블 데이터 조회
		Scan scan = new Scan();
		//table = conn.getTable(TableName.valueOf("people"));
		ResultScanner resultScanner = table.getScanner(scan);
		for (Result result = resultScanner.next(); result != null; result = resultScanner.next()) {
			
			for (Cell cell : result.rawCells()) {
				System.out.print("row=" + new String(CellUtil.cloneRow(cell)));
				System.out.print("|family(" + new String(CellUtil.cloneFamily(cell))+")");
				System.out.print("-->qualifier(" + new String(CellUtil.cloneQualifier(cell))+")");
				System.out.print(" value=" + new String(CellUtil.cloneValue(cell)));
				//System.out.print(" timestamp=" + cell.getTimestamp());
			}
			System.out.println("");
		}		
	}
}