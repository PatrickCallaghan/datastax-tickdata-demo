package com.datastax.tickdata.model;

public class TickData {
	private String key;
	private double value;
	
	public TickData(String key, double value){
		this.key = key;
		this.value = value;
	}

	public String getKey() {
		return key;
	}

	public double getValue() {
		return value;
	}

	@Override
	public String toString() {
		return "TickData [key=" + key + ", value=" + value + "]";
	}
}
