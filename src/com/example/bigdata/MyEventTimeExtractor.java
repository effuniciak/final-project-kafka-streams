package com.example.bigdata;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class MyEventTimeExtractor implements TimestampExtractor {
    @Override

    public long extract(final ConsumerRecord<Object, Object> record,
                        final long previousTimestamp) {
        long timestamp = 10;
//        String stringLine;
//
//        if (record.value() instanceof String) {
//            stringLine = (String) record.value();
//            if (AccessCsvRecord.lineIsCorrect(stringLine)) {
//                timestamp = AccessCsvRecord.parseFromCsvRow(stringLine).
//                        getTimestampInMillis();
//            }
//        }
        return timestamp;
    }
}