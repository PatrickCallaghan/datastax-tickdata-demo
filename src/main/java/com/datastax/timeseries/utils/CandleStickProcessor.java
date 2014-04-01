package com.datastax.timeseries.utils;

import org.joda.time.DateTime;

public class CandleStickProcessor {
	
	public static CandleStickSeries createCandleStickSeries(TimeSeries timeSeries, DateTime startTime, Periodicity periodicity){
		
		CandleStickSeries candleStickSeries = new CandleStickSeries(timeSeries.getSymbol());
		
		
		
		
		return candleStickSeries;
	}
	
}
