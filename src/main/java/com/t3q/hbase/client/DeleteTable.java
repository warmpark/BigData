package com.t3q.hbase.client;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class DeleteTable {
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

		// Disable, and then delete the table
		admin.disableTable(TableName.valueOf("people"));
		admin.deleteTable(TableName.valueOf("people"));
	}
}