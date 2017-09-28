package com.t3q.kafka;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;

public class ProducerExample {

	public static void main(String[] args) throws Exception {

		Properties props = new Properties();

		//props.put("metadata.broker.list","big01:9092,big02:9092,big03:9092");
		//props.put("serializer.class", "kafka.serializer.StringEncoder");
		
		props.put("bootstrap.servers", "big01:9092,big02:9092,big03:9092");// broker list 필수! 
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			
		
		Producer<String, String> producer  = new KafkaProducer<String,String>(props);	
		List<PartitionInfo> plist = producer.partitionsFor("test");
		
		for(int i=0; i<plist.size(); i++) {
			PartitionInfo pinfo = plist.get(i);
			Node node = pinfo.leader();
			System.out.println(node);
		}
		

		for(int i=0;  i<10000; i++){
			ProducerRecord<String, String> message = new ProducerRecord<String, String>("test", i+"",i+" @@2@@@@@@@@@@@@  Hello, World!");
			Thread.sleep(100);
			producer.send(message);
		}
		producer.close();

	}

}
