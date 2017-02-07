package com.infonova.opss.koma.reset;

import com.infonova.opss.koma.KomaSettings;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public interface Reset {
    void resetPartition(KomaSettings ks);
}
