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


    private String date;
    private double open;
    private double high;
    private double low;
    private double close;
    private double adjClose;
    private int volume;
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
        this.volume = Integer.parseInt(volume);
        this.stock = stock;
    }

    public static AccessCsvRecord parseFromCsvRow(String csvRow) {
        String[] dataArr = csvRow.split(",");

        if (dataArr.length != AccessCsvRecord.expectedDataLength) {
            throw new RuntimeException("Error parsing csvRow: " + csvRow);
        }

        return new AccessCsvRecord(dataArr[0], dataArr[1], dataArr[2], dataArr[3],
                dataArr[4], dataArr[5], dataArr[6], dataArr[7]);
    }


    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public double getOpen() {
        return clientIdentd;
    }

    public void setClientIdentd(String clientIdentd) {
        this.clientIdentd = clientIdentd;
    }

    public String getUserID() {
        return userID;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public String getDateTimeString() {
        return dateTimeString;
    }

    public void setDateTimeString(String dateTimeString) {
        this.dateTimeString = dateTimeString;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public void setResponseCode(int responseCode) {
        this.responseCode = responseCode;
    }

    public long getContentSize() {
        return contentSize;
    }

    public void setContentSize(long contentSize) {
        this.contentSize = contentSize;
    }

    @Override
    public String toString() {
        return String.format("%s %s %s [%s] \"%s %s %s\" %s %s", ipAddress, clientIdentd, userID,
                dateTimeString, method, endpoint, protocol, responseCode, contentSize);
    }

    public long getTimestampInMillis() {
        // 21/Jul/2014:9:55:27 -0800
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.US);
        Date date;
        try {
            date = sdf.parse(dateTimeString);
            return date.getTime();
        } catch (ParseException e) {
            return -1;
        }
    }
}
