package com.example.bigdata;

public class StockDataAggregator {
    public double close = 0;
    public double low = 0;
    public double high = 0;
    public double volume = 0;

    private static final String PATTERN = "%g %g %g %g";

    public static String createString(double close, double low, double high, double volume) {
        return String.format(StockDataAggregator.PATTERN, close, low, high, volume);
    }

    // pattern: "%g %g %g %g <- close low high volume"
    public static String stringToUpdatedString(String line, double close, double low, double high, double volume) {
        String[] splittedLine = line.split(" ");

        double newClose = Double.parseDouble(splittedLine[0]) + close;
        double newLow = Double.parseDouble(splittedLine[0]) + low;
        double newHigh = Double.parseDouble(splittedLine[0]) + high;
        double newVolume = Double.parseDouble(splittedLine[0]) + volume;

        return String.format(StockDataAggregator.PATTERN, newClose, newLow, newHigh, newVolume);
    }
}
