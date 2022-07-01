package com.monogdb.poctimeseries;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.TimeSeriesGranularity;
import com.mongodb.client.model.TimeSeriesOptions;

public class LoadRunner {

    private MongoClient mongoClient;
    Logger logger;

    private void prepareTimeSeriesSystem(POCTestOptions testOpts, POCTestResults results) {
        MongoDatabase db;
        MongoCollection<Document> coll;
        // Create indexes and suchlike
        db = mongoClient.getDatabase(testOpts.databaseName);
        coll = db.getCollection(testOpts.collectionName);
        coll.drop();  
        
        TimeSeriesOptions tsOptions = new TimeSeriesOptions("timestamp");
        tsOptions.granularity(TimeSeriesGranularity.SECONDS);
        tsOptions.metaField("metafield");
        CreateCollectionOptions collOptions = new CreateCollectionOptions().timeSeriesOptions(tsOptions);
       
        
        db.createCollection(testOpts.collectionName, collOptions);
        coll.createIndex(new Document("metafield", 1));
        coll.createIndex(new Document("timestamp", 1));
      //  coll.createIndex(new Document("sensorId", 1).append("timestamp",1));
        
        coll = db.getCollection(testOpts.collectionName);
        
        TestRecord testRecord = new TestRecord(testOpts);
        List<String> fields = testRecord.listFields();
//        for (int x = 0; x < testOpts.secondaryIdx; x++) {
//            coll.createIndex(new Document(fields.get(x), 1));
//        }
    }
    
    private void prepareBucketPatternSystem(POCTestOptions testOpts, POCTestResults results) {
        MongoDatabase db;
        MongoCollection<Document> coll;
        // Create indexes and suchlike
        db = mongoClient.getDatabase(testOpts.bpDatabaseName);
        coll = db.getCollection(testOpts.bpCollectionName);
        coll.drop();  
        coll.createIndex(new Document("sensorId", 1));
        coll.createIndex(new Document("sensorId", 1).append("timestamp",1));
        //TestRecordBP testRecord = new TestRecord(testOpts);
       // List<String> fields = testRecord.listFields();
//        for (int x = 0; x < testOpts.secondaryIdx; x++) {
//            coll.createIndex(new Document(fields.get(x), 1));
//        }
    }

  

    public void runLoad(POCTestOptions testOpts, POCTestResults testResults) {
    	if(testOpts.tscoll) {
    		prepareTimeSeriesSystem(testOpts, testResults);
    		
    		// Report on progress by looking at testResults
            POCTestReporter reporter = new POCTestReporter(testResults, mongoClient, testOpts);

            // Using a thread pool we keep filled
            ExecutorService testexec = Executors.newFixedThreadPool(testOpts.numThreads);

            // Allow for multiple clients to run -
            // Check for testOpts.threadIdStart - this should be an integer to start
            // the 'workerID' for each set of threads.
            int threadIdStart = testOpts.threadIdStart;
            logger.info("threadIdStart=" + threadIdStart);
            ArrayList<MongoWorker> workforce = new ArrayList<MongoWorker>();
            logger.info("Launching worker threads");
            for (int i = threadIdStart; i < (testOpts.numThreads); i++) {
                logger.info("Creating worker " + i);
                workforce.add(new MongoWorker(mongoClient, testOpts, testResults, i));
            }
            logger.info("Worker threads all started");

            ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
            executor.scheduleAtFixedRate(reporter, 0, testOpts.reportTime, TimeUnit.SECONDS);

            for (MongoWorker w : workforce) {
                testexec.execute(w);
            }

            testexec.shutdown();

            try {
                testexec.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                logger.info("All Threads Complete");
                executor.shutdown();
            } catch (InterruptedException e) {
                logger.error(e.getMessage());

            }

            // do final report
            reporter.finalReport();
    	}else {
    		prepareBucketPatternSystem(testOpts, testResults);
    		
    		// Report on progress by looking at testResults
            POCTestReporter reporter = new POCTestReporter(testResults, mongoClient, testOpts);

            // Using a thread pool we keep filled
            ExecutorService testexec = Executors.newFixedThreadPool(testOpts.numThreads);

            // Allow for multiple clients to run -
            // Check for testOpts.threadIdStart - this should be an integer to start
            // the 'workerID' for each set of threads.
            int threadIdStart = testOpts.threadIdStart;
            logger.info("threadIdStart=" + threadIdStart);
            ArrayList<MongoWorkerBP> workforce = new ArrayList<MongoWorkerBP>();
            logger.info("Launching worker threads");
            for (int i = threadIdStart; i < (testOpts.numThreads); i++) {
                logger.info("Creating worker " + i);
                workforce.add(new MongoWorkerBP(mongoClient, testOpts, testResults, i));
            }
            logger.info("Worker threads all started");

            ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
            executor.scheduleAtFixedRate(reporter, 0, testOpts.reportTime, TimeUnit.SECONDS);

            for (MongoWorkerBP w : workforce) {
                testexec.execute(w);
            }

            testexec.shutdown();

            try {
                testexec.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
                logger.info("All Threads Complete");
                executor.shutdown();
            } catch (InterruptedException e) {
                logger.error(e.getMessage());

            }

            // do final report
            reporter.finalReport();
    	}
       
        
    }

    LoadRunner(POCTestOptions testOpts) {
        logger = LoggerFactory.getLogger(LoadRunner.class);

        try {
            // For not authentication via connection string passing of user/pass only
            mongoClient = MongoClients.create(testOpts.connectionDetails);
        } catch (Exception ex) {
            StringWriter errors = new StringWriter();
            ex.printStackTrace(new PrintWriter(errors));
            logger.error(errors.toString());
        }
    }
}
