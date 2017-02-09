## koma
**K**afka **o**ffset **ma**nager which allows you to reset the offset for a given topic partition and group.

Supported reset types:
* beginning: Reset the offset to the beginning of the topic partition
* end: Reset the offset to the end of the topic partition
* offset: Reset the topic partition to a given offset
* timestamp: Reset the topic partition to a given timestamp

To run:
-------
1. Build the project with `mvn package`. This will create a tar.gz archive in the target folder which includes a control `koma.sh` shell script and a koma uber jar.
2. Perform a reset, e.g. `./koma.sh -r timestamp -b localhost:9092 -g logstash -t alert -p 0 -s "2017.02.08 12:55:00.000"`
3. For details see `./koma.sh -h`
