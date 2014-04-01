package com.datastax.tickdata;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Scanner;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;
import com.datastax.timeseries.utils.CandleStickProcessor;
import com.datastax.timeseries.utils.CandleStickSeries;
import com.datastax.timeseries.utils.Periodicity;
import com.datastax.timeseries.utils.PeriodicityProcessor;
import com.datastax.timeseries.utils.TechnicalAnalysis;
import com.datastax.timeseries.utils.TimeSeries;

public class QueryProcessor {

	private static Logger logger = LoggerFactory.getLogger(QueryProcessor.class);

	private DateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	private Scanner scanIn = new Scanner(System.in);

	public QueryProcessor() {

		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");

		TickDataDao dao = new TickDataDao(contactPointsStr.split(","));

		while (true) {

			System.out.print("Enter Symbol : ");
			String symbol = getInput("NASDAQ-AAPL-2014-03-28");

			System.out.print("Enter startTime : ");
			String startTime = getInput("2014-03-28 10:00:00");

			System.out.print("Enter endTime : ");
			String endTime = getInput("2014-03-28 10:20:00");

			System.out.print("Periodicity (HOUR, MINUTE) etc : ");
			String periodicity = getInput("MINUTE");

			System.out.print("Periodicity start time etc : ");
			String periodicityStartTime = getInput("2014-03-28 10:00:00");

			System.out.print("Moving Average period : ");
			String movingAveragePeriod = getInput("20");

			Timer timer = new Timer();
			timer.start();
			TimeSeries tickData = null;
	
			try {				
				tickData  = dao.getTickData(symbol, dateFormatter.parse(startTime).getTime(),
						dateFormatter.parse(endTime).getTime());
				System.out.println(tickData.toFormatterString());
			} catch (ParseException e) {
				e.printStackTrace();
			}

			//For analytics reverse series
			tickData.reverse();
			
			try {
				TimeSeries timeSeriesByPeriod = PeriodicityProcessor.getTimeSeriesByPeriod(tickData,
						Periodicity.valueOf(periodicity), dateFormatter.parse(periodicityStartTime).getTime());
				System.out.println("By Peridicity - " + periodicity);
				System.out.println(timeSeriesByPeriod.toFormatterString());
			} catch (ParseException e) {
				e.printStackTrace();
			}

			if (!movingAveragePeriod.equals("")) {
				TimeSeries movingAverage = TechnicalAnalysis.calculateMovingAverage(tickData,
						Integer.parseInt(movingAveragePeriod));

				System.out.println("Moving Average - " + movingAveragePeriod);
				System.out.println(movingAverage.toFormatterString());

			}
			
			
			CandleStickSeries candleStickSeries;
			try {
				candleStickSeries = CandleStickProcessor.createCandleStickSeries(tickData, new DateTime(
						dateFormatter.parse(startTime).getTime()), Periodicity.MINUTE);
				
				System.out.println(candleStickSeries);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			timer.end();
			
			logger.info("Got data and ran all analysis in " + timer.getTimeTakenMillis() + "ms");

		}
	}

	private String getInput(String defaultValue) {
		String value = scanIn.nextLine().trim();

		return value == null || value.equals("") ? defaultValue : value;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new QueryProcessor();
	}
}
