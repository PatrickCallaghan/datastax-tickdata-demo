package com.datastax.timeseries.utils;

public class CandleStick {

	private double high;
	private double low;
	private double open;
	private double close;
	
	public CandleStick(double high, double low, double open, double close) {
		super();
		this.high = high;
		this.low = low;
		this.open = open;
		this.close = close;
	}
	public double getHigh() {
		return high;
	}
	public double getLow() {
		return low;
	}
	public double getOpen() {
		return open;
	}
	public double getClose() {
		return close;
	}
	@Override
	public String toString() {
		return "CandleStick [high=" + high + ", low=" + low + ", open=" + open + ", close=" + close + "]";
	}
}
