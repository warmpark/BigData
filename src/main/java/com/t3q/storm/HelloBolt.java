package com.t3q.storm;

import java.util.Arrays;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class HelloBolt extends BaseBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		String value = tuple.getStringByField("say");
		System.out.println("Tuple value is" + value);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
	}
	
	
	public static void main(String[] args) {
		 //String zkUrls[] = new String[]{"big01:2181", "big02:2181", "big03:2181"};   
		 //System.out.println(Arrays.toString(zkUrls));
		 
		 String str = "big01:2181,big02:2181,big03:2181";
		 String[] aa = str.split(",");
		 System.out.println(Arrays.toString(aa));
		 
	}
	

}
