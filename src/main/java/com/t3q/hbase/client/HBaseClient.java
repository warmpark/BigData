package com.t3q.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
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
		config.addResource(
				new Path("/JavaOneShot/IDE/64/workspace/BigData/src/main/resources/conf/hbase/hbase-site.xml"));

		String tableName = "test2";
		String rowKey = "rowkey";
		String[] cfs = new String[] { "cf1", "cf2", "cf3" };
		String[] qfs = new String[] { "qualifier1", "qualifier2", "qualifier3" };
		String[] vals = new String[] { "value1", "value2", "value3" };

		HBaseAdmin.checkHBaseAvailable(config);
		System.out.println("connected to hbase");

		Connection conn = ConnectionFactory.createConnection(config);
		Admin admin = conn.getAdmin();

		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

		try {
			// 테이블 생성 & 컬럼 추가
			// HBaseAdmin hbase = null;
			for (int i = 0; i < cfs.length; i++) {
				HColumnDescriptor meta = new HColumnDescriptor(cfs[i].getBytes());
				System.out.println(meta);
				tableDescriptor.addFamily(meta);
			}
			admin.createTable(tableDescriptor); // create
		} catch (Exception e) {
			e.printStackTrace();
		}

		Table table = conn.getTable(TableName.valueOf(tableName));
		try {
			// 행 추가 (put)
			for (int i = 0; i < cfs.length; i++) {
				Put put = new Put(Bytes.toBytes(rowKey));
				put.addColumn(Bytes.toBytes(cfs[i]), Bytes.toBytes(qfs[i]), Bytes.toBytes(vals[i]));
				table.put(put);
			}
			table.close();
			
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

		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			// 행 삭제
			List<Delete> list = new ArrayList<Delete>();
			Delete d1 = new Delete(rowKey.getBytes());
			list.add(d1);
			table.delete(list);

			// disabling table named emp
			admin.disableTable(TableName.valueOf(tableName));
			// deleting table named emp
			admin.deleteTable(TableName.valueOf(tableName));
			table.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
