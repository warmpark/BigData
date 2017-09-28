package com.t3q.storm;

import java.util.Arrays;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.thrift.TException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

/**
 * //출처: http://blog.embian.com/108 [Embian Blog]
 * @author warmpark
 *
 */
public class HelloTopology {
	
	String zkUrls = "big01,big02,big03";
	String nimbusUrls = "big01,big02,big03";
	//String[] zkUrlsArray = zkUrls.split(",");
	//String[] nimbusArray = nimbusUrls.split(",");
	//String stormJarLocalUrl = "C:/Users/warmpark/.m2/repository/org/apache/storm/storm-core/1.1.0/storm-core-1.1.0.jar";
	String stormJarLocalUrl = "/JavaOneShot/IDE/64/workspace/BigData/target/BigData-0.0.1-SNAPSHOT.jar";
	
	
	public static void main(String args[]) {
		//new HelloTopology().deleteTopology("HelloTopology");
		new HelloTopology().deployTopology("HelloTopology_warmpark");
	}

	/**
	 * 
	 * @param topologyName
	 */
	public  void deployTopology(String topologyName) {
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("HelloSpout", new HelloSpout(), 2);
		builder.setBolt("HelloBolt", new HelloBolt(), 4).shuffleGrouping("HelloSpout");

		//Config config = new Config();
		Map<String,Object> config = Utils.readStormConfig(); // 이렇게 해야 해요
		//config.put(Config.NIMBUS_HOST, "9.119.84.179");
		config.put(Config.NIMBUS_THRIFT_PORT, 6627);
		//config.put(Config.NIMBUS_SEEDS, Arrays.asList("big01", "big02", "big03"));
		//config.put(Config.STORM_ZOOKEEPER_SERVERS,Arrays.asList("big01", "big02", "big03"));
		config.put(Config.STORM_ZOOKEEPER_SERVERS,Arrays.asList(zkUrls.split(",")));
		config.put(Config.NIMBUS_SEEDS, Arrays.asList(nimbusUrls.split(",")));
		config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
		config.put(Config.TOPOLOGY_WORKERS, 3);

		System.setProperty("storm.jar", stormJarLocalUrl);
		
		try {
			StormSubmitter.submitTopology(topologyName, config, builder.createTopology());
		} catch (AlreadyAliveException ae) {
			System.out.println(ae);
		} catch (InvalidTopologyException ie) {
			System.out.println(ie);
		} catch (AuthorizationException e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * @주의사항 - Map config = Utils.readStormConfig(); //이렇게 시작해야... 
	 *          System.setProperty("storm.jar",stormJarLocalUrl); //없어도 됨.
	 * @throws NotAliveException
	 * @throws AuthorizationException
	 * @throws TException
	 * 참고 https://stackoverflow.com/questions/20799178/how-to-programmatically-kill-a-storm-topology
	 */
	public  void deleteTopology(String topologyName) {
		
		Map<String,Object> config = Utils.readStormConfig(); // 이렇게 해야 해요
		//Config config = new Config(); // 이렇게 하면 안돼요. 
		System.out.println("=========================="+config);
		
		//Config config = new Config();
		config.put(Config.NIMBUS_THRIFT_PORT, 6627);
		//config.put(Config.NIMBUS_SEEDS, Arrays.asList("big01", "big02", "big03"));
		//config.put(Config.STORM_ZOOKEEPER_SERVERS,Arrays.asList("big01", "big02", "big03"));
		config.put(Config.STORM_ZOOKEEPER_SERVERS,Arrays.asList(zkUrls.split(",")));
		config.put(Config.NIMBUS_SEEDS, Arrays.asList(nimbusUrls.split(",")));
		config.put(Config.STORM_ZOOKEEPER_PORT, 2181);
		config.put(Config.TOPOLOGY_WORKERS, 3);

		System.setProperty("storm.jar",stormJarLocalUrl);
		System.out.println("=========================="+config);
		
		NimbusClient cc = NimbusClient.getConfiguredClient(config);
		
		System.out.println("=========================="+cc);
		Nimbus.Client client = cc.getClient();
		try {
			client.killTopology(topologyName);
			Thread.sleep(30000);
		} catch (NotAliveException e) {
			e.printStackTrace();
		} catch (AuthorizationException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
