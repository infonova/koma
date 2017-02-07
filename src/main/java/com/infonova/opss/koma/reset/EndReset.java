package com.infonova.opss.koma.reset;

import com.infonova.opss.koma.KomaSettings;
import com.infonova.opss.koma.reset.Reset;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EndReset implements Reset {

    private static final Logger log = LoggerFactory.getLogger(EndReset.class);

    public void resetPartition(KomaSettings ks) {
        log.info(String.format("Reset topic %s, partition %s to end for group %s", 
                    ks.getTopic(), ks.getPartition(), ks.getGroupId()));
    }
}
