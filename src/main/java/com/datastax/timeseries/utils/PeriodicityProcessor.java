package com.datastax.timeseries.utils;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;

import com.google.common.primitives.Longs;

public class PeriodicityProcessor {

	
	public static TimeSeries getTimeSeriesByPeriod (TimeSeries timeSeries, Periodicity periodicity, long startTime){
		
		List<Long> newDates = createDatesByPeriodicity(periodicity, startTime, timeSeries); //Use last date as enddate.
		double[] doubles = new double[newDates.size()];
		
		long[] oldDates = timeSeries.getDates();
		double[] values = timeSeries.getValues();
		
		int counter = 0;
		double lastValue = 0;
		
		for (int i=0; i < oldDates.length; i++){
			
			long date = oldDates[i];
			
			if (date > newDates.get(counter)){
				doubles[counter] = lastValue;

				while (date >= newDates.get(counter)){
					doubles[counter] = lastValue;
					counter ++;
				}
			}
			
			lastValue = values[i];
			
			if (i == oldDates.length-1){
				doubles[counter] = lastValue;
			}			
		}
		
		return new TimeSeries(timeSeries.getSymbol(), Longs.toArray(newDates), doubles);		
	}

	public static List<Long> createDatesByPeriodicity(Periodicity periodicity, long startTime, TimeSeries timeSeries) {
		
		long endTime = timeSeries.getDates()[timeSeries.getDates().length-1];
		
		List<Long> newDates = new ArrayList<Long>();
		
		//Only add if we have a valid value for the date.
		if (startTime > timeSeries.getDates()[0]){
			newDates.add(startTime);
		}

		while (startTime < endTime){
			startTime = startTime + periodicity.getDuration().getMillis();			

			if (startTime > timeSeries.getDates()[0]){
				newDates.add(startTime);
			}
		}
			
		return newDates;
	}
	
	
	public static void main(String[] args){
		
		long[] dates = new long[]{new DateTime(2014, 3, 26, 0, 2).getMillis(),
				new DateTime(2014, 3, 27, 1, 53).getMillis(),
				new DateTime(2014, 3, 27, 2, 2).getMillis(),
				new DateTime(2014, 3, 27, 2, 34).getMillis(),
				new DateTime(2014, 3, 27, 3, 12).getMillis(),
				new DateTime(2014, 3, 27, 3, 23).getMillis(),
				new DateTime(2014, 3, 27, 4, 24).getMillis(),
				new DateTime(2014, 3, 27, 5, 24).getMillis(),
				new DateTime(2014, 3, 27, 6, 35).getMillis(),
				new DateTime(2014, 3, 27, 9, 35).getMillis(),
				new DateTime(2014, 3, 27,11, 25).getMillis()
				};
		
		double[] values = {1,2,3,4,5,6,7,8,9,10,11};
		
		TimeSeries timeSeries = new TimeSeries ("test", dates, values);		
		
		List<Long> list = PeriodicityProcessor.createDatesByPeriodicity(Periodicity.HOUR, new DateTime(2014, 3, 27, 0, 0).getMillis() , timeSeries);
		
		for (Long date : list){
			
			System.out.println(new DateTime(date).toString());
			
		}
		
		TimeSeries seriesByPeriod = PeriodicityProcessor.getTimeSeriesByPeriod(timeSeries, Periodicity.HOUR, new DateTime(2014, 3, 27, 0, 0).getMillis());
		
		System.out.println(seriesByPeriod.toString());
		
		for (int i=0; i < seriesByPeriod.getDates().length; i++){
			
			System.out.println(new DateTime(seriesByPeriod.getDates()[i]).toString() + "-" + seriesByPeriod.getValues()[i]);			
		}
		
	}
}
