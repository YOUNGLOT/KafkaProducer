package com.rwk.kafka.producer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;

import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OnlineToKafkaProducer {
    private String bootstrapServers;
    private String lingerMs;
    private String timeoutMs;
    private String acks;
    private String keySerializer;
    private String valueSerializer;
    private String maxBlockMs;
    private String deliveryTimeoutMs;
    private String immediatelyFlush;
    private String autoFlush;
    private String autoFlushInterval;
    protected Logger logger = LoggerFactory.getLogger(getClass());
    protected Logger kafkaLogger = LoggerFactory.getLogger("kafkaLogger");
    private static Properties props;
    private org.apache.kafka.clients.producer.Producer producer;

    public static void main(String[] args) {
        try {
            OnlineToKafkaProducer o = new OnlineToKafkaProducer();
            o.init();
            o.send("TOPIC_17", "TOPIC_17");



        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void init() throws Exception {
        logger.debug("Kafka API init start.");
        props = new Properties();

//        props.setProperty("bootstrap.servers", bootstrapServers);
//        props.setProperty("linger.ms", lingerMs);
//        props.setProperty("timeout.ms", timeoutMs);
//        props.setProperty("acks", acks);
//        props.setProperty("key.serializer", keySerializer);
//        props.setProperty("value.serializer", valueSerializer);
//        props.setProperty("max.block.ms", maxBlockMs);
//        props.setProperty("delivery.timeout.ms", deliveryTimeoutMs);
//        props.setProperty("immediately.flush", immediatelyFlush);
//        props.setProperty("auto.flush", autoFlush);
//        props.setProperty("auto.flush.interval", autoFlushInterval);

        props.setProperty("bootstrap.servers", "rwkkafka01:47506,rwkkafka02:47507,rwkkafka03:47508");
//        props.setProperty("linger.ms", lingerMs);
//        props.setProperty("timeout.ms", timeoutMs);
        props.setProperty("acks", "0");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.setProperty("max.block.ms", maxBlockMs);
//        props.setProperty("delivery.timeout.ms", deliveryTimeoutMs);
        props.setProperty("immediately.flush", "false");
        props.setProperty("auto.flush", "true");
        props.setProperty("auto.flush.interval", "1000");


        producer = new KafkaProducer(props);

        new Thread(new Runnable() {
            @Override
            public void run() {
                while ("true".equals(autoFlush)) {
                    try {
                        Thread.sleep(Long.parseLong(autoFlushInterval));
                        flush();
                    } catch (Exception e) {
                        logger.error(e.getMessage());
                        e.printStackTrace();
                    }
                }
            }
        }).start();
        logger.debug("Kafka API init end.");
    }

    public void mapListToProduce(Map<String, String> header, List<Map<String, String>> data) throws Exception {
        logger.debug("Kafka API : mapListToProduce start. GUID [" + header.get("STD_ETXT_GLBL_ID") + "]");
        for (Map<String, String> map : data) {
            Map<String, String> dvoMap = stringToMap(map.get("dvoString"));
            dvoMap.put("KAFKA_IF_ID", "T_" + map.get("TBL_ID"));
            dvoMap.put("TBL_ID", map.get("TBL_ID"));
            dvoMap.put("RTM_DTA_PROCS_TPC", map.get("RTM_DTA_PROCS_TPC"));
            dvoMap.putAll(header);
            this.send(dvoMap.get("KAFKA_IF_ID"), mapToString(dvoMap));
        }
        logger.debug("Kafka API : mapListToProduce end. GUID [" + header.get("STD_ETXT_GLBL_ID") + "]");
    }

    public void mapToProduce(String interfaceId, Map header, Map data) throws Exception {
        data.putAll(header);
        String sendValue = mapToString(data);
        send(interfaceId, sendValue);

    }

    public void send(String topicName, String value) {
        try {
            logger.warn(value);
            producer.send(new ProducerRecord(topicName, value), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata,
                                         Exception e) {
                    if (e == null) {
                        // 정상처리
                        logger.debug("[Kafka SEND SUCCESS]" + recordMetadata.topic() + " || " + recordMetadata.toString());
                    } else {
                        logger.error(e.getMessage());
                        kafkaLogger.error(recordMetadata.topic() + " || " + recordMetadata.toString());
                        //e.printStackTrace();
                    }
                }
            });
            if ("true".equals(this.immediatelyFlush)) {
                producer.flush();
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            kafkaLogger.error(topicName + " || " + value);
            //e.printStackTrace();
        }
    }

    private String mapToString(Map map) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(map);
    }

    private Map<String, String> stringToMap(String in) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(in, new TypeReference<Map<String, String>>() {
        });
    }

    public void flush() throws Exception {
        producer.flush();
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public void setAcks(String acks) {
        this.acks = acks;
    }

    public void setLingerMs(String lingerMs) {
        this.lingerMs = lingerMs;
    }

    public void setTimeoutMs(String timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    public void setKeySerializer(String keySerializer) {
        this.keySerializer = keySerializer;
    }

    public void setValueSerializer(String valueSerializer) {
        this.valueSerializer = valueSerializer;
    }

    public void setMaxBlockMs(String maxBlockMs) {
        this.maxBlockMs = maxBlockMs;
    }

    public void setDeliveryTimeoutMs(String deliveryTimeoutMs) {
        this.deliveryTimeoutMs = deliveryTimeoutMs;
    }

    public void setImmediatelyFlush(String immediatelyFlush) {
        this.immediatelyFlush = immediatelyFlush;
    }

    public void setAutoFlush(String autoFlush) {
        this.autoFlush = autoFlush;
    }

    public void setAutoFlushInterval(String autoFlushInterval) {
        this.autoFlushInterval = autoFlushInterval;
    }
}
