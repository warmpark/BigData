package com.t3q.kafkastorm;

import java.io.File;
import java.util.Properties;

import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListenerAdapter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;



public class TestProducer {
	
	private static final int SLEEP = 500;
	public Producer<String,String> producer;
	
	public void setConfig(){
		Properties props = new Properties();
		props.put("bootstrap.servers", "big01:9092,big02:9092,big03:9092");// broker list 필수! 
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer  = new KafkaProducer<String,String>(props);
	}
	
	public static void main(String[] args) throws InterruptedException{ 
        TestProducer testProducer = new TestProducer();
        testProducer.setConfig();
        testProducer.run();
    }
	private void run() throws InterruptedException {
		OnlyLogListenter onlyLogListenter = new OnlyLogListenter(producer);
        Tailer.create(new File("C:/JavaOneShot/IDE/64/workspace/BigData/test.file"), onlyLogListenter,SLEEP);
        while(true){
        	Thread.sleep(SLEEP);
        }
    }
	
	
	/**
	 * 
	 * @author warmpark
	 *
	 */
	public class OnlyLogListenter extends TailerListenerAdapter{
		Producer<String,String> producer;
		public OnlyLogListenter(Producer<String,String> producer){
			this.producer = producer;
		}
		@Override
		public void handle(String line){
			 System.out.println(line);
			 ProducerRecord<String, String> message =new ProducerRecord<String, String>("onlytest",line);
		     producer.send(message);
		}
	}
	
	
	
}
