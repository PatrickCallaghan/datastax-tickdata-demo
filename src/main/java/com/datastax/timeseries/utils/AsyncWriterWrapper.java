package com.datastax.timeseries.utils;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

public class AsyncWriterWrapper {

	private Logger logger = LoggerFactory.getLogger(AsyncWriterWrapper.class);
	
	private List<Statement> statements;
	private Exception exception;
	private int retries = 3;
	private int counter = 0;

	public AsyncWriterWrapper(){
		this.statements = new ArrayList<Statement>();
	}
	
	public AsyncWriterWrapper(List<Statement> statements){
		if (statements==null){
			this.statements = statements;
		}else{
			this.statements = new ArrayList<Statement>();
		}
	}
	
	public void addStatement(Statement statement){
		this.statements.add(statement);
	}
	
	public boolean executeAsync(Session session){
		
		List<ResultSetFuture> results = new ArrayList<ResultSetFuture>();
		
		for (Statement statement : statements){
			results.add(session.executeAsync(statement));
		}
		
		//logger.info("Waiting for " + statements.size() + " to finish.");
		
		try{
			for (ResultSetFuture future : results){
				future.getUninterruptibly();
			}
		}catch (Exception e){
			this.exception = e;
			logger.error("Async Wrapper failed - " + e.getMessage());
		}
				
		return true;
	}
	
	public boolean replayExecute(Session session){
		counter++;
		return this.executeAsync(session);
	}
	
	public Exception getException(){
		return this.exception;
	}
	
	public boolean exhausted(){
		if (counter > retries){
			return true;
		}
		return false;
	}
}
