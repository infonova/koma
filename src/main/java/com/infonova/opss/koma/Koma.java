package com.infonova.opss.koma;

import com.infonova.opss.koma.constants.*;
import com.infonova.opss.koma.reset.*;

import org.apache.commons.cli.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Koma {

    private static final Logger log = LoggerFactory.getLogger(Koma.class);

    public static void main(String... args) {
        
        Options helperOptions = createHelperOptions();
        Options clientOptions = createClientOptions();

        try { 
            CommandLine helperCl = new DefaultParser().parse(helperOptions, args, true); 

            if (helperCl.getOptions().length != 0) {
                printHelp(clientOptions);
                System.exit(0);
            } 

            CommandLine komaCl = new DefaultParser().parse(clientOptions, args);

            ResetType resetType = ResetType.find(komaCl.getOptionValue(Constants.RESET_TO));
            if (resetType == null) {
               throw new ParseException(String.format("Unsupported reset type %s.", resetType));
            }
            
            if (Constants.OFFSET.equals(resetType.toString()) 
                    && komaCl.getOptionValue(Constants.OFFSET) == null) {
                throw new ParseException("Missing offset option");
            }

            if (Constants.TIMESTAMP.equals(resetType.toString()) 
                    && komaCl.getOptionValue(Constants.TIMESTAMP) == null) {
                throw new ParseException("Missing timestamp option");
            }

            KomaSettings komaSettings = new KomaSettings();
            
            komaSettings.setBootstrapServers(komaCl.getOptionValue(Constants.BOOTSTRAP_SERVERS));
            komaSettings.setGroupId(komaCl.getOptionValue(Constants.GROUP_ID));
            komaSettings.setKeyDeserializer(komaCl.getOptionValue(Constants.KEY_DESERIALIZER, 
                        Constants.DEFAULT_KEY_DESERIALIZER));
            komaSettings.setValueDeserializer(komaCl.getOptionValue(Constants.VALUE_DESERIALIZER, 
                        Constants.DEFAULT_VALUE_DESERIALIZER));
            komaSettings.setTopic(komaCl.getOptionValue(Constants.TOPIC));
            komaSettings.setPartition(Integer.parseInt(komaCl.getOptionValue(Constants.PARTITION)));
            komaSettings.setOffset(Long.parseLong(komaCl.getOptionValue(Constants.OFFSET, String.valueOf(Long.MAX_VALUE))));
            komaSettings.setTimestamp(komaCl.getOptionValue(Constants.TIMESTAMP, "2222.12.31 23:59:59.000"));
            komaSettings.setTimestampFormat(komaCl.getOptionValue(Constants.TIMESTAMP_FORMAT, "yyyy.MM.dd HH:mm:ss.SSS"));

            Reset reset = resetType.getReset();
            reset.resetPartition(komaSettings);
            
        } catch(ParseException pe) {
            log.error("Error parsing command line options:", pe);
            printHelp(clientOptions);
            System.exit(1);
        }
    }
    
    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("koma", options);
    }

    private static Options createHelperOptions() {
        Options options = new Options(); 

        Option help = Option.builder("h")
            .required(false)
            .hasArg(false)
            .longOpt(Constants.HELP)
            .build();
        options.addOption(help);

        return options;
    }

    private static Options createClientOptions() {
        Options options = new Options(); 
        
        Option bootstrapServers = Option.builder("b")
            .type(String.class)
            .required(true)
            .hasArg()
            .argName("server:port")
            .longOpt(Constants.BOOTSTRAP_SERVERS)
            .desc("The bootstrap server list. List items are separated by comma.")
            .build();
        options.addOption(bootstrapServers);

        Option groupId = Option.builder("g")
            .type(String.class)
            .required(true)
            .hasArg()
            .argName("name")
            .longOpt(Constants.GROUP_ID)
            .desc("The group id.")
            .build();
        options.addOption(groupId);

        Option topic = Option.builder("t")
            .type(String.class)
            .required(true)
            .hasArg()
            .argName("name")
            .longOpt(Constants.TOPIC)
            .desc("The topic name.")
            .build();
        options.addOption(topic);

        Option partition = Option.builder("p")
            .type(Number.class)
            .required(false)
            .hasArg()
            .argName("id")
            .longOpt(Constants.PARTITION)
            .desc("The partition id.")
            .build();
        options.addOption(partition);

        Option resetTo = Option.builder("r")
            .type(String.class)
            .required(true)
            .hasArg()
            .argName("type")
            .longOpt(Constants.RESET_TO)
            .desc("The reset type (beginning, end, offset or timestamp).")
            .build();
        options.addOption(resetTo);

        Option offset = Option.builder("o")
            .type(Number.class)
            .required(false)
            .hasArg()
            .argName("id")
            .longOpt(Constants.OFFSET)
            .desc("The reset offset when using reset type offset.")
            .build();
        options.addOption(offset);

        Option timestamp = Option.builder("s")
            .type(String.class)
            .required(false)
            .hasArg()
            .argName("date")
            .longOpt(Constants.TIMESTAMP)
            .desc("The reset date when using reset type timestamp (e.g. 2017.02.05 14:30:55.666).")
            .build();
        options.addOption(timestamp);

        Option timestampFormat = Option.builder("f")
            .type(String.class)
            .required(false)
            .hasArg()
            .argName("format")
            .longOpt(Constants.TIMESTAMP_FORMAT)
            .desc("The timestamp format. The default is  yyyy.MM.dd HH:mm:ss.SSS")
            .build();
        options.addOption(timestampFormat);

        Option keyDeserializer = Option.builder("k")
            .type(String.class)
            .required(false)
            .hasArg()
            .argName("class")
            .longOpt(Constants.KEY_DESERIALIZER)
            .desc("The key deserializer. Defaults to org.apache.kafka.common.serialization.StringDeserializer")
            .build();
        options.addOption(keyDeserializer);

        Option valueDeserializer = Option.builder("v")
            .type(String.class)
            .required(false)
            .hasArg()
            .argName("class")
            .longOpt(Constants.VALUE_DESERIALIZER)
            .desc("The value deserializer. Defaults to org.apache.kafka.common.serialization.StringDeserializer")
            .build();
        options.addOption(valueDeserializer);

        return options;
    }
}
