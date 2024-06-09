package com.example.bigdata;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class FinalProjectKafkaStreams {

    public static void main(String[] args) throws Exception {
        String bootstrapServersConfig = args[0];
        String sourceTopic = args[1];
        String outputEtlTopic = args[2];
        String outputAnomaliesTopic = args[3];
        int D = Integer.parseInt(args[4]);
        int P = Integer.parseInt(args[5]);
        System.out.println(String.format("%s %s %s %s %s %s", args[0], args[1], args[2], args[3]
            ,args[4], args[5]));

        Properties config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersConfig);
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "final-project-kafka-streams");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MyEventTimeExtractor.class);

        final Serde<String> stringSerde = Serdes.String();

        final StreamsBuilder builder = new StreamsBuilder();

        // key is always null so we could skip de-serializing it
        KStream<byte[], String> textLines = builder
                .stream(sourceTopic, Consumed.with(null, stringSerde));

        KStream<byte[], AccessCsvRecord> csvDataStream = textLines
                .filter((key, value) -> AccessCsvRecord.lineIsCorrect(value))
                .mapValues(value -> AccessCsvRecord.parseFromCsvRow(value));

        csvDataStream.peek((k ,v) -> System.out.println(v));
//        csvDataStream.peek((k ,v) -> System.out.println(v.getTimestampInMillis()));

        /*
            1) group it by stock symbol and window by month (hardcoded 30 days)
            2) aggregate by Close, Low, High, Volume
         */

//        KTable<Windowed<String>, String> dataAggregatedByStockAndMonth = csvDataStream.map(
//                (key, value) -> new KeyValue<>(value.getStock(), value)
//        ).groupByKey().windowedBy(TimeWindows.of(Duration.ofDays(30))).aggregate(
//                () -> StockDataAggregator.createString(0, 0, 0, 0),
//                (k, v, aggregate) -> StockDataAggregator.stringToUpdatedString(aggregate, v.getClose(), v.getLow(), v.getHigh(), v.getVolume()),
//                Materialized.<String, String, WindowStore<Bytes, byte[]>>as("fp-etl-store")
//                        .withKeySerde(stringSerde)
//                        .withValueSerde(stringSerde)
//        );
//
//        dataAggregatedByStockAndMonth.toStream().to(outputEtlTopic);

        KStream<byte[], String> outputDataAsString = csvDataStream.mapValues(v -> v.toString());
        outputDataAsString.to(outputEtlTopic, Produced.with(null, stringSerde));

        final Topology topology = builder.build();
        System.out.println(topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, config);

        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            System.out.println("Streams started correctly");
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);

    }
}

