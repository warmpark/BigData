package com.t3q.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseClient {

	public static void main(String[] args) throws Exception {

		Configuration config = HBaseConfiguration.create();
		config.clear();

		// configuration -#1. 직접 설정
		// config.set("hbase.master", "big01:600");
		// config.set("hbase.zookeeper.quorum","big01:2181,big02:2181,big03:2181");
		// config.set("hbase.zookeeper.property.clientPort", "2181");

		// configuration -#2. hbase-site.xml로 부터.
		config.addResource(new Path("/JavaOneShot/IDE/64/workspace/BigData/src/main/resources/conf/hbase/hbase-site.xml"));

		String tableName = "test2";
		String rowKey = "rowkey";
		String[] cfs = new String[] { "cf1", "cf2", "cf3" };
		String[] qfs = new String[] { "qualifier1", "qualifier2", "qualifier3" };
		String[] vals = new String[] { "value1", "value2", "value3" };

		HBaseAdmin.checkHBaseAvailable(config);
		System.out.println("connected to hbase");
		HTable table = new HTable(config, "bunty");

		Scan scan = new Scan();

		// 테이블 생성 & 컬럼 추가
		HBaseAdmin hbase = null;
		try {
			hbase = new HBaseAdmin(config);
			HTableDescriptor desc = new HTableDescriptor(tableName);
			for (int i = 0; i < cfs.length; i++) {
				HColumnDescriptor meta = new HColumnDescriptor(cfs[i].getBytes());
				desc.addFamily(meta);
			}
			hbase.createTable(desc); // create
		} finally {
			if (hbase != null)
				hbase.close();
		}

		table = new HTable(config, tableName);
		try {
			// 행 추가 (put)
			for (int i = 0; i < cfs.length; i++) {
				Put put = new Put(Bytes.toBytes(rowKey));
				put.add(Bytes.toBytes(cfs[i]), Bytes.toBytes(qfs[i]),Bytes.toBytes(vals[i]));
				table.put(put);
			}

			// 테이블 데이터 조회
			Scan s = new Scan();
			ResultScanner rs = table.getScanner(s);
			for (Result r : rs) {
				for (KeyValue kv : r.raw()) {
					System.out.println("row:" + new String(kv.getRow()) + "");
					System.out.println("family:" + new String(kv.getFamily())+ ":");
					System.out.println("qualifier:"+ new String(kv.getQualifier()) + "");
					System.out.println("value:" + new String(kv.getValue()));
					System.out.println("timestamp:" + kv.getTimestamp() + "");
					System.out.println("-------------------------------------------");
				}
			}

			// 행 삭제
			List list = new ArrayList();
			Delete d1 = new Delete(rowKey.getBytes());
			list.add(d1);
			table.delete(list);

			// 테이블 drop
			HBaseAdmin admin = new HBaseAdmin(config);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);

		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
	
	public void xxx() {
		// Do this on prepare()

		Configuration config = HBaseConfiguration.create();

		//configuration -#1. 직접 설정
		config.set("hbase.master", "big01:600");
		config.set("hbase.zookeeper.quorum","big01:2181,big02:2181,big03:2181");
		config.set("hbase.zookeeper.property.clientPort", "2181");

		try {

			HBaseAdmin hbaseAdmin = new HBaseAdmin(config);
			HTable transactionHistoryTable = null;

			if (hbaseAdmin.tableExists("TransactionHistory")) {
				transactionHistoryTable = new HTable(config,"TransactionHistory");

			} else {

				HTableDescriptor tableDescriptor = new HTableDescriptor("TransactionHistory");
				HColumnDescriptor cfColumnFamily = new HColumnDescriptor("Transactions".getBytes());
				tableDescriptor.addFamily(cfColumnFamily);
				hbaseAdmin.createTable(tableDescriptor);
				transactionHistoryTable = new HTable(config,"TransactionHistory");

			}

			hbaseAdmin.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		// Do this on execute()
		//Put transactionToPersist = new Put(Bytes.toBytes(transaction.getTransactionId()));
		//transactionToPersist.add(Bytes.toBytes("Transactions"),Bytes.toBytes("accountNumber"),Bytes.toBytes(transaction.getAccountNumber()));
	}

}
