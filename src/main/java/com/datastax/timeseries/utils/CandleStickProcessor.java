package com.datastax.timeseries.utils;

import java.util.List;

import org.joda.time.DateTime;

import cern.colt.list.DoubleArrayList;

public class CandleStickProcessor {

	public static CandleStickSeries createCandleStickSeries(TimeSeries timeSeries, DateTime startTime,
			Periodicity periodicity) {

		CandleStickSeries candleStickSeries = new CandleStickSeries(timeSeries.getSymbol());
		List<Long> candleStickTimePoints = PeriodicityProcessor.createDatesByPeriodicity(periodicity,
				startTime.getMillis(), timeSeries);

		long[] oldDates = timeSeries.getDates();
		double[] oldValues = timeSeries.getValues();

		int counter = 0;
		double lastValue = 0;
		
		DoubleArrayList doubles = new DoubleArrayList();

		for (int i = 0; i < oldDates.length; i++) {

			long date = oldDates[i];
			doubles.add(lastValue);

			if (date > candleStickTimePoints.get(counter)){				
				
				CandleStick candleStick = createCandleStickFromArrayList(doubles);
				
				if (candleStick!=null){
					candleStick.setStartTime(candleStickTimePoints.get(counter).longValue());
					candleStickSeries.addCandleStick(candleStick);
					doubles.clear();
				}
				
				while (date >= candleStickTimePoints.get(counter)) {
					counter++;
				}
			}

			lastValue = oldValues[i];

			if (i == oldDates.length - 1) {
				doubles.add(lastValue);							
			}
			
		}
		//Add current candle stick.		
		CandleStick candleStick = createCandleStickFromArrayList(doubles);
		candleStick.setStartTime(candleStickTimePoints.get(counter).longValue());
		if (candleStick!=null)
		candleStickSeries.addCandleStick(candleStick);


		return candleStickSeries;
	}

	public static CandleStick createCandleStickFromArrayList(DoubleArrayList doubles) {
		if (doubles == null) return null;
				
		System.out.println("Creating candlestick from " + doubles.toString());
		
		doubles.trimToSize();
		
		if (doubles.size() == 0) return null;
		
		double open = doubles.get(0);
		double close = doubles.get(doubles.size()-1);
		
		doubles.sort();
		
		double low = doubles.get(0);
		double high = doubles.get(doubles.size()-1);
				
		return new CandleStick(high, low, open, close);
	}

}
