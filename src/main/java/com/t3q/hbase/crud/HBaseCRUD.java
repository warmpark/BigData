package com.t3q.hbase.crud;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 참고 : https://www.tutorialspoint.com/hbase/
 * 
 * @author warmpark
 * 
 */
public class HBaseCRUD {
	Configuration conf = HBaseConfiguration.create();

	public HBaseCRUD() {
		// configuration -#1. 직접 설정
		conf.set("hbase.master", "big01:600");
		conf.set("hbase.zookeeper.quorum", "big01,big02,big03");
		conf.set("hbase.zookeeper.property.clientPort", "2181");

		// configuration -#2. hbase-site.xml로 부터.
		// conf.addResource(new
		// Path("/JavaOneShot/IDE/64/workspace/BigData/src/main/resources/conf/hbase/hbase-site.xml"));
	}

	public static void main(String[] args) {
		HBaseCRUD crud = new HBaseCRUD();
		try {
			crud.createTable();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		try {
			crud.putData();
			crud.readData();
			crud.scanData();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		/*
		try {
			crud.deleteData();
			crud.deleteTable();
		} catch (IOException e) {
			e.printStackTrace();
		}
		*/
	}

	/**
	 * https://www.tutorialspoint.com/hbase/hbase_create_table.htm 
	 * create ‘<table name>’,’<column family>’
	 * 
	 * @throws IOException
	 */
	public void createTable() throws IOException {

		Connection conn = ConnectionFactory.createConnection(conf);
		Admin admin = conn.getAdmin();

		// Instantiating table descriptor class
		HTableDescriptor tableDescriptor = new HTableDescriptor(
				TableName.valueOf("emp"));

		// Adding column families to table descriptor
		tableDescriptor.addFamily(new HColumnDescriptor("personal"));
		tableDescriptor.addFamily(new HColumnDescriptor("professional"));

		// Execute the table through admin
		admin.createTable(tableDescriptor);
		System.out.println(" Table created ");
	}

	/**
	 * insert or update
	 * https://www.tutorialspoint.com/hbase/hbase_create_data.htm put ’
	 * <table name>
	 * ’,’row1’,’<colfamily:colname>’,’<value>’ hbase(main):005:0> put
	 * 'emp','1','personal:name','raju' 0 row(s) in 0.6600 seconds
	 * hbase(main):006:0> put 'emp','1','personal:city','hyderabad' 0 row(s) in
	 * 0.0410 seconds hbase(main):007:0> put
	 * 'emp','1','professional:designation','manager' 0 row(s) in 0.0240 seconds
	 * hbase(main):007:0> put 'emp','1','professional:salary','50000' 0 row(s)
	 * in 0.0240 seconds
	 * 
	 * 
	 * https://www.tutorialspoint.com/hbase/hbase_update_data.htm
	 * 
	 * 
	 * @throws IOException
	 */
	public void putData() throws IOException {
		// Instantiating configuration class
		Connection conn = ConnectionFactory.createConnection(conf);
		// HTable table = new HTable(conf, "Acadgild");

		// Instantiating HTable class
		Table table = conn.getTable(TableName.valueOf("emp"));

		// accepts a row name. [row1 is row id]
		Put p = new Put(Bytes.toBytes("row1"));
		

		// adding values using add() method
		// accepts column family name, qualifier/row name ,value
		p.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"),Bytes.toBytes("raju"));
		p.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("city"),Bytes.toBytes("hyderabad"));
		p.addColumn(Bytes.toBytes("professional"),Bytes.toBytes("designation"), Bytes.toBytes("manager"));
		p.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("salary"),Bytes.toBytes("90000"));

		// Saving the put Instance to the HTable.
		table.put(p);
		System.out.println("data inserted");

	}

	/**
	 * https://www.tutorialspoint.com/hbase/hbase_read_data.htm 
	 * get ’<table name>’,’rowid’ 
	 * hbase(main):012:0> get 'emp', 'row1'
	 * 
	 * hbase> get 'table name', ‘rowid’, {COLUMN => ‘column family:column name ’}
	 * hbase(main):015:0> get 'emp', 'row1', {COLUMN ⇒ 'personal:name'}
	 * 
	 * @throws IOException
	 */
	public void readData() throws IOException {
		// Instantiating configuration class
		Connection conn = ConnectionFactory.createConnection(conf);
		// HTable table = new HTable(conf, "Acadgild");

		// Instantiating HTable class
		Table table = conn.getTable(TableName.valueOf("emp"));

		// Instantiating Get class
		Get g = new Get(Bytes.toBytes("row1"));

		// Reading the data
		Result result = table.get(g);

		// Reading values from Result class object
		byte[] value = result.getValue(Bytes.toBytes("personal"),Bytes.toBytes("name"));

		byte[] value1 = result.getValue(Bytes.toBytes("personal"),Bytes.toBytes("city"));

		// Printing the values
		String name = Bytes.toString(value);
		String city = Bytes.toString(value1);

		System.out.println("name: " + name + " city: " + city);
	}
	
	
	/**
	 * https://www.tutorialspoint.com/hbase/hbase_scan.htm 
	 * scan ‘<table name>’ 
	 * hbase(main):010:0> scan 'emp'
	 * 
	 * @throws IOException
	 */
	public void scanData() throws IOException {
		// Instantiating configuration class
		Connection conn = ConnectionFactory.createConnection(conf);
		// HTable table = new HTable(conf, "Acadgild");

		// Instantiating HTable class
		Table table = conn.getTable(TableName.valueOf("emp"));

		// Instantiating the Scan class
		Scan scan = new Scan();

		// Scanning the required columns
		scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"));
		scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("city"));

		// Getting the scan result
		ResultScanner scanner = table.getScanner(scan);

		// Reading values from scan result
		for (Result result = scanner.next(); result != null; result = scanner.next()){
			System.out.println("Found row : " + result);
		}
		// closing the scanner
		scanner.close();
	}
	

	/**
	 * https://www.tutorialspoint.com/hbase/hbase_delete_data.htm delete ‘
	 * <table name>
	 * ’, ‘<row>’, ‘<column name >’, ‘<time stamp>’ delete 'emp', '1',
	 * 'personal:city', 1417521848375
	 * 
	 * @throws IOException
	 */
	public void deleteData() throws IOException {
		// Instantiating configuration class
		Connection conn = ConnectionFactory.createConnection(conf);
		// HTable table = new HTable(conf, "Acadgild");

		// Instantiating HTable class
		Table table = conn.getTable(TableName.valueOf("emp"));

		// Instantiating Delete class
		Delete delete = new Delete(Bytes.toBytes("row1"));
		delete.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"));
		delete.addFamily(Bytes.toBytes("professional"));

		// deleting the data
		table.delete(delete);

		// closing the HTable object
		table.close();
		System.out.println("data deleted.....");
	}

	/*
	 * https://www.tutorialspoint.com/hbase/hbase_drop_table.htm
	 * hbase(main):018:0> disable 'emp' 0 row(s) in 1.4580 seconds
	 * hbase(main):019:0> drop 'emp' 0 row(s) in 0.3060 seconds
	 * hbase(main):020:0>exists 'emp'
	 */
	public void deleteTable() throws IOException {
		Connection conn = ConnectionFactory.createConnection(conf);

		Admin admin = conn.getAdmin();
		// disabling table named emp
		admin.disableTable(TableName.valueOf("emp"));
		// deleting table named emp
		admin.deleteTable(TableName.valueOf("emp"));
		System.out.println("Table deleted");
	}

}
