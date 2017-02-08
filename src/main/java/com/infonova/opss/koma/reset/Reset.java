package com.infonova.opss.koma.reset;

import com.infonova.opss.koma.KomaSettings;
import com.infonova.opss.koma.constants.Constants;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.util.Properties;

public interface Reset {
    public void resetPartition(KomaSettings ks);

    default public KafkaConsumer createConsumer(KomaSettings ks) {
        Properties props = new Properties();
        props.put(Constants.BOOTSTRAP_SERVERS, ks.getBootstrapServers()); 
        props.put(Constants.GROUP_ID, ks.getGroupId()); 
        props.put(Constants.KEY_DESERIALIZER, ks.getKeyDeserializer()); 
        props.put(Constants.VALUE_DESERIALIZER, ks.getValueDeserializer()); 

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        return consumer;
    } 
}
