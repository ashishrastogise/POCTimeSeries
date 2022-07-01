package com.monogdb.poctimeseries;

import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.descending;

import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.WriteModel;

public class MongoWorkerBP implements Runnable {

    private MongoClient mongoClient;
    private MongoCollection<Document> coll;
    private ArrayList<MongoCollection<Document>> colls;
    private POCTestOptions testOpts;
    private POCTestResults testResults;
    private int workerID;
    private int sequence;
    private Random rng;
    private ZipfDistribution zipf;
    private boolean workflowed = false;
    private boolean zipfian = false;
    private String workflow;
    private int workflowStep = 0;
    private ArrayList<Document> keyStack;
    private int lastCollection;
    private int maxCollections;
   
    Logger logger;

    

    MongoWorkerBP(MongoClient c, POCTestOptions t, POCTestResults r, int id) {
        mongoClient = c;
        logger = LoggerFactory.getLogger(MongoWorkerBP.class);
        // Ping
        c.getDatabase("admin").runCommand(new Document("ping", 1));
        testOpts = t;
        testResults = r;
        workerID = id;
        MongoDatabase db = mongoClient.getDatabase(testOpts.bpDatabaseName);
        maxCollections = testOpts.numcollections;
        String baseCollectionName = testOpts.bpCollectionName;
        if (maxCollections > 1) {
            colls = new ArrayList<MongoCollection<Document>>();
            lastCollection = 0;
            for (int i = 0; i < maxCollections; i++) {
                String str = baseCollectionName + i;
                colls.add(db.getCollection(str));
            }
        } else {
            coll = db.getCollection(baseCollectionName);
        }

        // id
        sequence = getHighestID();

      
        rng = new Random();
  

    }

    private int getNextVal(int mult) {
        int rval;
        if (zipfian) {
            rval = zipf.sample();
        } else {
                long now = ZonedDateTime.now().toInstant().toEpochMilli();
                if(mult ==0) { 
                	mult=1; 
                }
                rval = (int) (now % mult);
        }
        return rval;
    }

    private int getHighestID() {
        int rval = 0;

        
        Document query = new Document();

        // TODO Refactor the query for 3.0 driver
        Document limits = new Document("$gt", new Document("w", workerID));
        limits.append("$lt", new Document("w", workerID + 1));

        query.append("_id", limits);

        Document myDoc = coll.find(query).projection(include("_id")).sort(descending("_id")).first();
        if (myDoc != null) {
            Document id = (Document) myDoc.get("_id");
            rval = id.getInteger("i") + 1;
        }
        return rval;
    }

    // This one was a major rewrite as the whole Bulk Ops API changed in 3.0

    private void flushBulkOps(List<WriteModel<Document>> bulkWriter) {
        // Time this.
      
        Date starttime = new Date();

        // This is where ALL writes are happening
        // So this can fail part way through if we have a failover
        // In which case we resubmit it

        boolean submitted = false;
        BulkWriteResult bwResult = null;

        while (!submitted && !bulkWriter.isEmpty()) { // can be empty if we removed a Dupe key error
            try {
                submitted = true;
                bwResult = coll.bulkWrite(bulkWriter);
            } catch (Exception e) {
                // We had a problem with this bulk op - some may be completed, some may not

                // I need to resubmit it here
                String error = e.getMessage();

                // Check if it's a sup key and remove it
                Pattern p = Pattern.compile("dup key: \\{ : \\{ w: (.*?), i: (.*?) }");

                Matcher m = p.matcher(error);
                if (m.find()) {
                    logger.debug("Duplicate Key");
                    int thread = Integer.parseInt(m.group(1));
                    int uniqid = Integer.parseInt(m.group(2));
                    logger.debug(" ID = " + thread + " " + uniqid);
                    boolean found = false;
                    for (Iterator<? super WriteModel<Document>> iter = bulkWriter.listIterator(); iter.hasNext();) {
                        // Check if it's a InsertOneModel

                        Object o = iter.next();
                        if (o instanceof InsertOneModel<?>) {
                            @SuppressWarnings("unchecked")
                            InsertOneModel<Document> a = (InsertOneModel<Document>) o;
                            Document id = (Document) a.getDocument().get("_id");

                            int opthread = id.getInteger("w");
                            int opid = id.getInteger("i");

                            if (id.getInteger("i") == uniqid) {
                                logger.debug(
                                        " Removing " + thread + " " + uniqid + " from bulkop as" + " already inserted");
                                iter.remove();
                                found = true;
                            }
                        }
                    }
                    if (!found) {
                        logger.warn("Cannot find failed op in batch!");
                    }
                } else {
                    // Some other error occurred - possibly MongoCommandException,
                    // MongoTimeoutException
                    logger.warn(e.getClass().getSimpleName() + ": " + error);
                    // Print a full stacktrace since we're in debug mode
                    if (testOpts.debug)
                        e.printStackTrace();
                }
                logger.debug("No result returned");
                submitted = false;
            }
        }

        Date endtime = new Date();

        Long taken = endtime.getTime() - starttime.getTime();

        int icount = bwResult.getInsertedCount();
      
        // If the bulk op is slow - ALL those ops were slow
        recordSlowOps("inserts", taken, icount);
       
        testResults.RecordOpsDone("inserts", icount);

    }

    


    private void recordSlowOps(String opname, Long taken, int count) {

        for (int i = 0; testOpts.slowThresholds != null && testOpts.slowThresholds.length > i; i++) {
            int slowThreshold = testOpts.slowThresholds[i];
            if (taken > slowThreshold) {
                // testResults.RecordSlowOp("inserts", icount, 50);
                testResults.RecordSlowOp(opname, count, i);
            }
        }
    }

    private TestRecordBP createNewRecord(String timeStamp, Document data) {
    	return new TestRecordBP(workerID, sequence++, timeStamp, data);
    }

    private TestRecordBP insertNewRecord(List<WriteModel<Document>> bulkWriter) {
    	Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy HH:mm");
        String timeStamp = sdf.format(cal.getTime());
    	
    	TestRecordBP testRecord = new TestRecordBP();
    	Document data = testRecord.generateDataFields(new Document(), sequence, workflowStep, sequence, maxCollections, lastCollection, workerID);
    	
    	String sensorId = "sensor-"+workerID;
    	
    	Document udpatedDocument = updateSingleRecord(bulkWriter, data, timeStamp, sensorId);
    	if(udpatedDocument == null) {
    		TestRecordBP tr = createNewRecord(timeStamp, data);
            bulkWriter.add(new InsertOneModel<Document>(tr.internalDoc));
            return tr;
    	}
        return null;
        
    }

    private Document updateSingleRecord(List<WriteModel<Document>> bulkWriter, Document data, String timeStamp, String sensorId) {
        // Key Query
      
        Document query = new Document();
        Document change;
        query.append("timestamp", timeStamp).append("sensorId", sensorId);
    
        change = new Document().append(
                "$push", new Document("value", data));
        Document updatedDocument = this.coll.findOneAndUpdate(query, change); // These are immediate not batches
        
        testResults.RecordOpsDone("updates", 1);
        
        return updatedDocument;
    }
    
    public void run() {
        // Use a bulk inserter - even if ony for one
        List<WriteModel<Document>> bulkWriter;

        try {
            bulkWriter = new ArrayList<WriteModel<Document>>();
            int bulkops = 0;

            int c = 0;
            logger.debug("Worker thread " + workerID + " Started.");
            while (testResults.GetSecondsElapsed() < testOpts.duration) {
                c++;
                // Timer isn't granullar enough to sleep for each
               
                if (!workflowed) {
                    logger.debug("Random op");
                    // Choose the type of op
                    int allops = testOpts.insertops;

                    /*
                     * Change - no longer a ratio of operations, that wasn't helpful as a 50:50
                     * split would be limited to the speed of the slower operation now a ratio of
                     * TIME - 50% of the time it will start an operation of type X
                     */

                    int randop = getNextVal(allops);

                    if (randop < testOpts.insertops) {
                        TestRecordBP tr = insertNewRecord(bulkWriter);
                        if(tr != null) {
                        	bulkops++;
                        }
                        
                    } 
                }

                if (c % testOpts.batchSize == 0) {
                    if (bulkops > 0) {
                        flushBulkOps(bulkWriter);
                        bulkWriter.clear();
                        bulkops = 0;
                        
                    }
                }
                if (testOpts.opsPerSecond > 0) {
                    
                    Thread.sleep(testOpts.granularityInMS);
                }

            }

        } catch (Exception e) {
        	e.printStackTrace();
            logger.warn("Error: " + e.getMessage());
            if (testOpts.debug)
                e.printStackTrace();
        }
    }
}
