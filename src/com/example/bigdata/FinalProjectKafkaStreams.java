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
        String outputTopic = args[2];
        int D = Integer.parseInt(args[3]);
        int P = Integer.parseInt(args[4]);

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

        final Serde<StockDataAggregator> stockDataAggregatorSerde =

        /*
            1) group it by stock symbol and window by month (hardcoded 30 days)
            2) aggregate by Close, Low, High, Volume
         */


        KTable<Windowed<String>, String> dataAggregatedByStockAndMonth = csvDataStream.map(
                (key, value) -> new KeyValue<>(value.getStock(), value)
        ).groupByKey().windowedBy(TimeWindows.of(Duration.ofDays(30))).aggregate(
                () -> StockDataAggregator.createString(0, 0, 0, 0),
                (k, v, aggregate) -> StockDataAggregator.stringToUpdatedString(aggregate, v.getClose(), v.getLow(), v.getHigh(), v.getVolume()),
                Materialized.<String, String, WindowStore<Bytes, byte[]>>as("fp-etl-store")
                        .withKeySerde(stringSerde)
                        .withValueSerde(stringSerde)
        );

//        KGroupedStream<byte[], AccessCsvRecord> groupedCsvDataStream = csvDataStream.groupByKey();
//        KTable<byte[], AccessCsvRecord> monthlyAndAggregatedData =
//                groupedCsvDataStream.aggregate()

//        KTable<String, String> recordDates = csvDataStream.map((key, value) -> KeyValue.pair(key, value.getDate()))

//        KTable<Windowed<String>, Long> ipCounts = csvDataStream
//                .map((key, value) -> KeyValue.pair(???, ""))
//                .groupByKey()
//                .windowedBy(TimeWindows.of(Duration.???(10)) /* time-based window */)
//                .count();
//
//        KTable<String, String> difficultIps = ipCounts.toStream()
//                .filter((key, value) -> value > ???)
//                .map((key, value) -> KeyValue.pair(key.key(), String.valueOf(value)))
//                .groupByKey()
//                .reduce((aggValue, newValue) -> newValue,
//                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("???"));

//        KStream<String, String> keyIpValueEndpoint = apacheLogStream
//                .map((key, value) -> KeyValue.pair(???, value.getEndpoint()));

        csvDataStream
                .toStream(???, (enpoint, howmany) -> enpoint + "," + howmany)
                .to(outputTopic);

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
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);

    }
}

