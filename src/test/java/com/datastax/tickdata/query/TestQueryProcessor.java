package com.datastax.tickdata.query;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import cern.colt.list.DoubleArrayList;

import com.datastax.tickdata.TickDataDao;
import com.datastax.timeseries.utils.CandleStick;
import com.datastax.timeseries.utils.CandleStickProcessor;
import com.datastax.timeseries.utils.Periodicity;
import com.datastax.timeseries.utils.PeriodicityProcessor;
import com.datastax.timeseries.utils.TechnicalAnalysis;
import com.datastax.timeseries.utils.TimeSeries;

public class TestQueryProcessor {
	private DateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	
	@Test
	public void testQuery(){
		TickDataDao dao = new TickDataDao(new String[]{"localhost"});

		TimeSeries tickData = dao.getTickData("NASDAQ-AAPL-2014-03-28");
		System.out.println(tickData.toFormatterString());
		
		Assert.assertTrue(tickData.getDates().length > 100);
	}
	
	@Test
	public void testPeriodicity(){
		TickDataDao dao = new TickDataDao(new String[]{"localhost"});

		TimeSeries tickData = dao.getTickData("NASDAQ-AAPL-2014-03-28");
		
		//Need to reverse for Periodicity
		tickData.reverse();
		
		TimeSeries byPeriod = PeriodicityProcessor.getTimeSeriesByPeriod(tickData, Periodicity.MINUTE, new DateTime(2014, 03, 28, 10, 00).getMillis());
		
		System.out.println(byPeriod.toFormatterString());		
	}
	
	@Test 
	public void testCandleStick(){
		
		DoubleArrayList doubles = new DoubleArrayList();
		doubles.add(2);
		doubles.add(1);
		doubles.add(3);
		doubles.add(6);
		doubles.add(4);
		doubles.add(3);
		doubles.add(4);
		doubles.trimToSize();
		CandleStick candleStick = CandleStickProcessor.createCandleStickFromArrayList(doubles);
		System.out.println(candleStick);
		
		Assert.assertEquals(6, candleStick.getHigh(), .001);
		Assert.assertEquals(1, candleStick.getLow(), .001);
		Assert.assertEquals(2, candleStick.getOpen(), .001);
		Assert.assertEquals(4, candleStick.getClose(), .001);		
	}
	
	@Test
	public void testMovingAverage(){
		TickDataDao dao = new TickDataDao(new String[]{"localhost"});

		TimeSeries tickData;
		
		try {
			tickData = dao.getTickData("NASDAQ-AAPL-2014-03-28", dateFormatter.parse("2014-03-28 10:15:02").getTime(),
					dateFormatter.parse("2014-03-28 10:19:02").getTime());
		} catch (ParseException e) {
			throw new RuntimeException(e.getMessage());
		}
		
		//Need to reverse for Technical analysis
		tickData.reverse();
		
		TimeSeries byPeriod = TechnicalAnalysis.calculateMovingAverage(tickData, 40);
		
		System.out.println(byPeriod.toFormatterString());		
	}
}
