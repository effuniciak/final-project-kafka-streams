package com.example.bigdata;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AccessCsvRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger logger = Logger.getLogger("Access");
    // Example Apache log line: TODO: refactor logger/get rid of it
    // 127.0.0.1 - - [21/Jul/2014:9:55:27 -0800] "GET /home.html HTTP/1.1" 200 2048

    public static int expectedDataLength = 8;

    private static final String CSV_ENTRY_PATTERN =
            "(\\d\\d\\d\\d-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\d\\.\\d\\d\\dZ)(,\\d+\\.\\d+)(,\\d+\\.\\d+)(,\\d+\\.\\d+)(,\\d+\\.\\d+)(,\\d+\\.\\d+)(,\\d+\\.\\d+),[A-Z]+";
    private static final Pattern PATTERN = Pattern.compile(CSV_ENTRY_PATTERN);


    private String date;
    private double open;
    private double high;
    private double low;
    private double close;
    private double adjClose;
    private double volume;
    private String stock;

    private AccessCsvRecord(String date, String open, String high,
                            String low, String close, String adjClose,
                            String volume, String stock) {
        this.date = date;
        this.open = Double.parseDouble(open);
        this.high = Double.parseDouble(high);
        this.low = Double.parseDouble(low);
        this.close = Double.parseDouble(close);
        this.adjClose = Double.parseDouble(adjClose);
        this.volume = Double.parseDouble(volume);
        this.stock = stock;
    }

    public static AccessCsvRecord parseFromCsvRow(String csvRow) {
        if (!AccessCsvRecord.lineIsCorrect(csvRow)) {
            throw new RuntimeException("Error parsing csvRow: " + csvRow);
        }

        String[] dataArr = csvRow.split(",");

        return new AccessCsvRecord(dataArr[0], dataArr[1], dataArr[2], dataArr[3],
                dataArr[4], dataArr[5], dataArr[6], dataArr[7]);
    }

    public static boolean lineIsCorrect(String line) {
        Matcher m = PATTERN.matcher(line);
        return m.find();
    }

    public String getDate() {
        return this.date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public double getOpen() {
        return this.open;
    }

    public void setOpen(double open) { this.open = open; }

    public double getHigh() {
        return this.high;
    }

    public void setHigh(double high) { this.high = high; }

    public double getLow() {
        return this.low;
    }

    public void setLow(double low) { this.low = low; }

    public double getClose() {
        return this.close;
    }

    public void setClose(double close) { this.close = close; }

    public double getAdjClose() {
        return this.adjClose;
    }

    public void setAdjClose(double adjClose) { this.adjClose = adjClose; }

    public double getVolume() {
        return this.volume;
    }

    public void setVolume(double volume) { this.volume = volume; }
public String getStock() {
    return this.stock;
}

    public void setStock(String stock) { this.stock = stock; }


    @Override
    public String toString() {
        return String.format("%s, %s, %s, %s, %s, %s, %s, %s",this.date, this.open, this.low,
                this.high, this.close, this.adjClose, this.volume, this.stock);
    }

    public long getTimestampInMillis() {
        // 21/Jul/2014:9:55:27 -0800
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US);
        Date date;

        try {
            date = sdf.parse(this.date);
            return date.getTime();
        } catch (ParseException e) {
            return -1;
        }
    }
}
