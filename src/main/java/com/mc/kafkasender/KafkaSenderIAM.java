package com.mc.kafkasender;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Date;
import java.util.Properties;

//prerequiste
//1. export KAFKA_OPTS=-Daws.accessKeyId=<accesskey> -Daws.secretKey=<secret>
  2. install kafka binaries in ec2 
  3. create topic in msk using ec2 instance in same subnset as msk. 
  

public class KafkaSenderIAM {

    public static void main(String [] args){

        final Producer<Long, String> producer = createProducer();
        final ProducerRecord<Long, String> record = new ProducerRecord("test","Hello world" + new Date());
               producer.send(record);
            producer.flush();
            producer.close();
    }

    private static Producer<Long, String> createProducer() {
        Properties props = new Properties();
		//TODO replace with cluster url
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "mskcluster:9098");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put("security.protocol","SASL_SSL");
        props.put("sasl.mechanism","AWS_MSK_IAM");
        props.put("sasl.jaas.config","software.amazon.msk.auth.iam.IAMLoginModule required awsDebugCreds=true;");
        props.put("sasl.client.callback.handler.class","software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        return new KafkaProducer<>(props);
    }
}
