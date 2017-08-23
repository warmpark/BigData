package com.t3q.kafkastorm;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


public class DoctypeCountBolt extends BaseBasicBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Map<String,Integer> docMap = new HashMap<String,Integer>();
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		System.out.println("==========="+input);
		String doctype = input.getStringByField("subdoctype");
		
		Integer count = docMap.get(doctype);
		if(count == null)count = 0;
		
		count++;
		
		docMap.put(doctype, count);
		System.out.println(docMap);
		collector.emit(new Values(docMap));
		
	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("docmap"));
	}
}


//출처: http://blog.embian.com/108 [Embian Blog]