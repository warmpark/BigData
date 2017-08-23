package com.t3q.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class HelloTopologyLocal {
	public static void main(String args[]) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("HelloSpout", new HelloSpout(), 2);
		builder.setBolt("HelloBolt", new HelloBolt(), 4).shuffleGrouping("HelloSpout");

		Config conf = new Config();
		conf.setDebug(true);
		LocalCluster cluster = new LocalCluster();

		cluster.submitTopology("HelloTopologyLocal", conf,builder.createTopology());
		Utils.sleep(10000);
		// kill the LearningStormTopology
		cluster.killTopology("HelloTopologyLocal");
		// shutdown the storm test cluster
		cluster.shutdown();
	}

}
