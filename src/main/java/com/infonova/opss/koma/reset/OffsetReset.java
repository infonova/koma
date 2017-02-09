package com.infonova.opss.koma.reset;

import com.infonova.opss.koma.KomaSettings;
import com.infonova.opss.koma.reset.Reset;
import com.infonova.opss.koma.constants.Constants;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OffsetReset implements Reset {

    private static final Logger log = LoggerFactory.getLogger(OffsetReset.class);

    public void resetPartition(KomaSettings ks) {
        log.info(String.format("Reset topic %s, partition %s to offset %s for group %s", 
                    ks.getTopic(), ks.getPartition(), ks.getOffset(), ks.getGroupId()));

        KafkaConsumer<String, String> consumer = createConsumer(ks);

        TopicPartition topicPartition = new TopicPartition(ks.getTopic(), ks.getPartition());
        consumer.assign(Collections.singletonList(topicPartition));
        consumer.seek(topicPartition, ks.getOffset());

        OffsetAndMetadata offsetMeta = new OffsetAndMetadata(consumer.position(topicPartition));
        Map<TopicPartition, OffsetAndMetadata> m = new HashMap<>();
        m.put(topicPartition, offsetMeta);

        log.info(String.format("Setting to offset %s.", offsetMeta.offset()));
        consumer.commitSync(m);
        consumer.close();
    }
}
