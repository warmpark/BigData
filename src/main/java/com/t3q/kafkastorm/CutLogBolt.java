package com.t3q.kafkastorm;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class CutLogBolt extends BaseBasicBolt{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		System.out.println("==========="+input);
		String[] splitArray = input.getString(0).split(";");
		String key = "";
		String doctype = "";
		for(int i = 0; i < splitArray.length; i++){
			if(splitArray[i].contains("key"))
				key  = splitArray[i];
			if(splitArray[i].contains("doctype"))
				doctype = splitArray[i];
		}
		System.out.format("CutLogBolt %s=%s", key,doctype);
		collector.emit(new Values(key,doctype));
	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key","doctype"));
	}

	
	public static void main(String[] args) {
		
		String[] splitArray = new String("key1 : doctype1;key2 : doctype2;key3 : doctype3").split(";");
		for (int i = 0; i < splitArray.length; i++) {
			System.out.println(splitArray[i]);
		}
		String key = "";
		String doctype="";
		for(int i = 0; i < splitArray.length; i++){
			if(splitArray[i].contains("key"))
				key  = splitArray[i];
			if(splitArray[i].contains("doctype"))
				doctype = splitArray[i];
		}
		Values values = new Values(key,doctype);
		System.out.println(values);
	}
	

}


//출처: http://blog.embian.com/108 [Embian Blog]