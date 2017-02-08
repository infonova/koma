package com.infonova.opss.koma.reset;

import com.infonova.opss.koma.KomaSettings;
import com.infonova.opss.koma.reset.Reset;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimestampReset implements Reset {

    private static final Logger log = LoggerFactory.getLogger(TimestampReset.class);

    public void resetPartition(KomaSettings ks) {
        log.info(String.format("Reset topic %s, partition %s to timestamp %s for group %s", 
                    ks.getTopic(), ks.getPartition(), ks.getTimestamp(), ks.getGroupId()));

        KafkaConsumer<String, String> consumer = createConsumer(ks);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(ks.getTimestampFormat());
        LocalDateTime dateTime = LocalDateTime.parse(ks.getTimestamp(), formatter);
        ZonedDateTime zonedDateTime = dateTime.atZone(ZoneId.systemDefault());
        long millis = zonedDateTime.toInstant().toEpochMilli();

        TopicPartition topicPartition = new TopicPartition(ks.getTopic(), ks.getPartition());
        consumer.assign(Arrays.asList(topicPartition));

        Map<TopicPartition, Long> query = new HashMap<>();
        query.put(topicPartition, millis);

        Map<TopicPartition, OffsetAndTimestamp> result = consumer.offsetsForTimes(query);

        Map<TopicPartition, OffsetAndMetadata> m = new HashMap<>();
        result.entrySet()
                .stream()
                .filter(entry -> entry.getValue() != null)
                .forEach(entry -> {
                    log.info(String.format("Setting to offset %s.", entry.getValue().offset()));
                    m.put(topicPartition, new OffsetAndMetadata(entry.getValue().offset()));
                });
        consumer.commitSync(m);
        consumer.close(); 
    }
}
