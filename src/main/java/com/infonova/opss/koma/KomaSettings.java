package com.infonova.opss.koma;

public class KomaSettings {
    private String bootstrapServers;
    private String groupId;
    private String keyDeserializer;
    private String valueDeserializer;
    private String topic;
    private int partition;
    private long offset;
    private String timestamp;
    private String timestampFormat;

    public void setBootstrapServers(String bs) {
        this.bootstrapServers = bs;
    }

    public void setGroupId(String gi) {
        this.groupId = gi;
    }

    public void setKeyDeserializer(String kd) {
        this.keyDeserializer = kd;
    }

    public void setValueDeserializer(String vd) {
        this.valueDeserializer = vd;
    }

    public void setTopic(String t) {
        this.topic = t;
    }

    public void setPartition(int p) {
        this.partition = p;
    }

    public void setOffset(long o) {
        this.offset = o;
    }

    public void setTimestamp(String ts) {
        this.timestamp = ts;
    }

    public void setTimestampFormat(String f) {
        this.timestampFormat = f;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getKeyDeserializer() {
        return keyDeserializer;
    }

    public String getValueDeserializer() {
        return valueDeserializer;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getTimestampFormat() {
        return timestampFormat;
    }
}
