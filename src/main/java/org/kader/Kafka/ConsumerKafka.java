package org.kader.Kafka;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Hello world!
 *
 */
public class ConsumerKafka {
	private String BROKER_KAFKA_URL="localhost:9092";
	private String TOPIC_NAME="test";
		
    public static void main( String[] args ){
        new ConsumerKafka();
    }
    
    public  ConsumerKafka() {
    	
    	Properties properties=new Properties();
    	properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BROKER_KAFKA_URL);
    	properties.put(ConsumerConfig.GROUP_ID_CONFIG,"sample-group-test");
    	properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
    	properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");
    	properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,30000);
    	properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
    	properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
    	
    	KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
    	kafkaConsumer.subscribe(Collections.singletonList(TOPIC_NAME));
    	Executors.newScheduledThreadPool(1).scheduleAtFixedRate(()->{
    		System.out.println("----------------TEST-----------------");
    		ConsumerRecords<String,String>consumerRecords=kafkaConsumer.poll(Duration.ofMillis(1000));
    		consumerRecords.forEach(cr->{
    			System.out.println(" key => " +cr.key() + "value => " +cr.value() +" offset => " +cr.offset() +"partition " +cr.topic());
    		});
    		
    	}, 3000, 3000,TimeUnit.MILLISECONDS);
    	
    	
    }
    
    
    
    
}
