package com.datastax.tickdata;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.timeseries.utils.Periodicity;
import com.datastax.timeseries.utils.PeriodicityProcessor;
import com.datastax.timeseries.utils.TechnicalAnalysis;
import com.datastax.timeseries.utils.TimeSeries;

public class QueryProcessor {
	private static Logger logger = LoggerFactory.getLogger(QueryProcessor.class);
	
	private DateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

	public QueryProcessor() {

		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		
		TickDataDao dao = new TickDataDao(contactPointsStr.split(","));

		while(true){
			
			System.out.print("Enter Symbol : ");
			Scanner scanIn = new Scanner(System.in);
		    String symbol  = scanIn.nextLine().trim();
		    
		    if (symbol.equals("")){
		    	symbol = "NASDAQ-AAPL-2014-03-28";
		    }
		    
		    System.out.print("Enter startTime : ");
		    String startTime= scanIn.nextLine().trim();
	
		    System.out.print("Enter endTime : ");
		    String endTime  = scanIn.nextLine().trim();

		    System.out.print("Periodicity (HOUR, MINUTE) etc : ");
		    String periodicity  = scanIn.nextLine().trim();

		    System.out.print("Periodicity start time etc : ");
		    String periodicityStartTime  = scanIn.nextLine().trim();

		    
		    System.out.print("Moving Average period : ");
		    String movingAveragePeriod  = scanIn.nextLine().trim();
		    
		    TimeSeries tickData = null;
		    if  (startTime.equals("") && endTime.equals("")){
		    	tickData = dao.getTickData(symbol);
		    	
		    	System.out.println(tickData.toFormatterString());
		    }else{
		    	try {
					tickData = dao.getTickData(symbol, dateFormatter.parse(startTime).getTime(), dateFormatter.parse(endTime).getTime());
			    	System.out.println(tickData.toFormatterString());
				} catch (ParseException e) {
					e.printStackTrace();
				}
		    }
		    
		    try {
				TimeSeries timeSeriesByPeriod = PeriodicityProcessor.getTimeSeriesByPeriod(tickData, Periodicity.valueOf(periodicity), dateFormatter.parse(periodicityStartTime).getTime());
				System.out.println("By Peridicity - " + periodicity);
				System.out.println(timeSeriesByPeriod.toFormatterString());
			} catch (ParseException e) {
				e.printStackTrace();
			}
		    
		    if (!movingAveragePeriod.equals("")){
		    	TimeSeries movingAverage = TechnicalAnalysis.calculateMovingAverage(tickData, Integer.parseInt(movingAveragePeriod));
		    	
				System.out.println("Moving Average - " + movingAveragePeriod);
				System.out.println(movingAverage.toFormatterString());

		    }
		    
		}
	}
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new QueryProcessor();
	}
}
