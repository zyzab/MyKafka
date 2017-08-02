package com.zyz.kafka.clients;

import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

/**
 * Created by zyz on 2017/8/2.
 */
public class ProducerInAction {


    private static Logger LOGGER = LoggerFactory.getLogger(ProducerInAction.class);


    public static Properties getProperties(String filePath){
        InputStream in = ClassLoader.getSystemResourceAsStream(filePath);
        Properties properties = new Properties();
        try {
            properties.load(in);
        }catch (Exception e){
            LOGGER.error("load file=>{}",filePath,e);
        }
        return properties;
    }

    public static Producer<String,String> getProducer(){
        Producer<String,String> producer = null;
        Properties properties = getProperties("kafkaProducer.properties");
        if(null==properties||properties.isEmpty()){
            LOGGER.error("init Producer error,Properties file load fail! ");
            return producer;
        }
        producer = new KafkaProducer<String, String>(properties);
        return producer;
    }




    public static void main(String[] args) {
        Producer producer = null;
        try{
            LOGGER.info("start producer message...");
            producer = getProducer();
            ProducerRecord producerRecord = null;
            for(int i = 0; i < 100; i++){
                producerRecord = new ProducerRecord<String, String>("test", Integer.toString(i), Integer.toString(i));
                producer.send(producerRecord,new Callback() {
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if(e != null){
                            LOGGER.error("ask record error",e);
                        }
                        LOGGER.info("The offset of the record we just sent is: partition=>{},offset=>{} ",metadata.partition(),metadata.offset());
                    }
                });
            }
        }catch (Exception e){
            LOGGER.error("producer start fail e:",e);
        } finally{
            if(null!=producer){
                producer.close();
            }
            LOGGER.info("stop producer");
        }

    }
}
