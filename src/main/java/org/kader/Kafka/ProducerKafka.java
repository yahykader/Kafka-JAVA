package org.kader.Kafka;

import java.time.Duration;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerKafka {
	
	private String BROKER_KAFKA_URL="localhost:9092";
	private String TOPIC_NAME="test";
    private String CLIENT_ID="client-producer-1";

	public static void main(String[] args) {
		new ProducerKafka(); 

	}
	
	public ProducerKafka(){
		Properties properties=new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_KAFKA_URL);
		properties.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		KafkaProducer<String,String>kafkaProducer=new KafkaProducer<String,String>(properties);
		Random rd=new Random();
    	Executors.newScheduledThreadPool(1).scheduleAtFixedRate(()->{
    		String key=String.valueOf(rd.nextInt(1000));
    		String value=String.valueOf(rd.nextDouble()*10000);
    		System.out.println("----------------TEST-----------------");
    		kafkaProducer.send(new ProducerRecord<String,String>(TOPIC_NAME,key,value),(metadata,ex)->{
    			System.out.println("SEND MSG"+value+"Partition ** "+metadata.partition()+" offset +" +metadata.offset());
    			
    		});
    		
    	}, 1000,1000,TimeUnit.MILLISECONDS);
		
		
	}

}
