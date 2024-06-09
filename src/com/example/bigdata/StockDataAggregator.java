package com.example.bigdata;

public class StockDataAggregator {
    public double close = 0;
    public double low = 0;
    public double high = 0;
    public double volume = 0;

    public StockDataAggregator returnUpdated(double close, double low, double high, double volume) {
        this.close += close;
        this.low += low;
        this.high += high;
        this.volume += volume;
        return this;
    }
}
