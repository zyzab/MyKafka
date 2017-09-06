package com.zyz.kafka.clients;

import com.zyz.kafka.utill.PropertiesUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by zyz on 2017/9/6.
 */
public class ComsumerInAction {

    private static Logger LOGGER = LoggerFactory.getLogger(ProducerInAction.class);

    public static Consumer<String,String> getConsumer(){
        Consumer<String,String> consumer = null;
        Properties properties = PropertiesUtils.getProperties("kafkaProducer.properties");
        if(null==properties||properties.isEmpty()){
            LOGGER.error("init consumer error,Properties file load fail! ");
            return consumer;
        }
        consumer = new KafkaConsumer<String, String>(properties);
        return consumer;
    }

    public static void main(String[] args) {
        Consumer consumer = null;
        try{
            LOGGER.info("start consumer message...");
            consumer = getConsumer();

            consumer.subscribe(Arrays.asList("test1","test2"));

            ConsumerRecords<String,String> consumerRecords = null;

            while(true){
                consumerRecords = consumer.poll(1000);
                if(null==consumerRecords||consumerRecords.isEmpty()){
                    LOGGER.info("consumer poll is null !");
                    continue;
                }
                for(ConsumerRecord<String,String> consumerRecord : consumerRecords){
                    LOGGER.info("The offset of the record we just sent is: partition=>{},offset=>{},key=>{},value=>{} ",consumerRecord.partition(),consumerRecord.offset(),
                            consumerRecord.key(),consumerRecord.value());
                }
            }
        }catch (Exception e){
            LOGGER.error("consumer start fail e:",e);
        } finally{
            if(null!=consumer){
                consumer.close();
            }
            LOGGER.info("stop consumer");
        }
    }

}
