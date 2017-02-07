package com.infonova.opss.koma.reset;

import com.infonova.opss.koma.KomaSettings;
import com.infonova.opss.koma.reset.Reset;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OffsetReset implements Reset {

    private static final Logger log = LoggerFactory.getLogger(OffsetReset.class);

    public void resetPartition(KomaSettings ks) {
        log.info(String.format("Reset topic %s, partition %s to offset %s for group %s", 
                    ks.getTopic(), ks.getPartition(), ks.getOffset(), ks.getGroupId()));
    }
}
