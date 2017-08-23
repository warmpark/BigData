package com.t3q.kafkastorm;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class ClassifyKeyBolt extends BaseBasicBolt{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;


	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		System.out.println("==========="+input);
		String[] splitdoctype = input.getStringByField("doctype").split(":");
		String[] splitkey = input.getStringByField("key").split(":");
		if(splitkey.length == 2 && splitdoctype.length == 2){
			String doctype  = splitdoctype[1].trim();
			String key  = splitkey[1].trim();
			System.out.format("ClassifyKeyBolt %s=%s", key,doctype);
			collector.emit(new Values(key + ":" + doctype));
		}
	}


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("subdoctype"));
	}
}


//출처: http://blog.embian.com/108 [Embian Blog]