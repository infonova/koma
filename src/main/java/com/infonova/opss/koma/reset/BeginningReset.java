package com.infonova.opss.koma.reset;

import com.infonova.opss.koma.KomaSettings;
import com.infonova.opss.koma.reset.Reset;
import com.infonova.opss.koma.constants.Constants;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BeginningReset implements Reset {

    private static final Logger log = LoggerFactory.getLogger(BeginningReset.class);

    public void resetPartition(KomaSettings ks) {
        log.info(String.format("Reset topic %s, partition %s to beginning for group %s", 
                    ks.getTopic(), ks.getPartition(), ks.getGroupId()));
        
        KafkaConsumer<String, String> consumer = createConsumer(ks);

        TopicPartition topicPartition = new TopicPartition(ks.getTopic(), ks.getPartition());
        consumer.assign(Collections.singletonList(topicPartition));
        consumer.seekToBeginning(
                Stream.of(topicPartition)
                    .collect(Collectors.toList()));

        OffsetAndMetadata offsetMeta = new OffsetAndMetadata(consumer.position(topicPartition));
        Map<TopicPartition, OffsetAndMetadata> m = new HashMap<>();
        m.put(topicPartition, offsetMeta);

        log.info(String.format("Setting to offset %s.", offsetMeta.offset()));
        consumer.commitSync(m);
        consumer.close();
    }
}
