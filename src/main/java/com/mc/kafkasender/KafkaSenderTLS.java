package com.mc.kafkasender;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Date;
import java.util.Properties;

public class KafkaSenderTLS {

    public static void main(String [] args){

        final Producer<Long, String> producer = createProducer();
        final ProducerRecord<Long, String> record = new ProducerRecord("test1","Hello world" + new Date());
               producer.send(record);
            producer.flush();
            producer.close();
    }

    private static Producer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "mskcluster:9094");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put("security.protocol","SSL");
        props.put("ssl.keystore.location","C:\\Users\\CManohar\\Desktop\\aws\\client.jks");
        props.put("ssl.keystore.password","novell");
        props.put("ssl.truststore.location","C:\\Users\\CManohar\\Desktop\\aws\\cacerts");
        props.put("ssl.truststore.password","changeit");
        return new KafkaProducer<>(props);
    }
}
