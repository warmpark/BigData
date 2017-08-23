package com.t3q.hbase.client;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class DeleteTable {
	public static void main(String[] args) throws IOException {
		Configuration config = HBaseConfiguration.create();
		// configuration
		config.set("hbase.master", "big01:600");
		config.set("hbase.zookeeper.quorum", "big01:2181,big02:2181,big03:2181");
		config.set("hbase.zookeeper.property.clientPort", "2181");

		// Create an admin object using the config
		HBaseAdmin admin = new HBaseAdmin(config);

		// Disable, and then delete the table
		admin.disableTable("people");
		admin.deleteTable("people");
	}
}