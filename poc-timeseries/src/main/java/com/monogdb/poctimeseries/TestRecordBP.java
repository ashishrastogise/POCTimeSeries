package com.monogdb.poctimeseries;


import java.util.*;

import org.bson.BsonBinarySubType;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

import de.svenjacobs.loremipsum.LoremIpsum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


//A Test Record is a MongoDB Record Object that is self populating

public class TestRecordBP {
	
	static Logger logger = LoggerFactory.getLogger(TestRecordBP.class);
	
	Document internalDoc;
	private Random rng;

	private static String loremText = null;
    private String messageCode[] = {"001", "011", "021", "031", "041", "051", "061", "071", "081","091"};
    private String action[] = {"generate","view","cancel","rejected","deleted","failed","processed","expired","invalid","notreachable"};
    private String message[] = {"Data generated",
    		"Data viewed",
    		"Data canceled",
    		"Data rejected",
    		"Data deleted",
    		"Data failed",
    		"Data processed",
    		"Data expired",
    		"Data invalid",
    		"Not Reachable"};

	
	

	private void AddOID(int workerid, int sequence) {
		//Document oid = new Document("w",workerid).append("i", sequence);
		internalDoc.append("_id", new ObjectId());
	}

	// Just so we always know what the type of a given field is
	// Useful for querying, indexing etc

	private static int getFieldType(int fieldno) {
		if (fieldno == 0) {
			return 0; // Int
		}

		if (fieldno == 1) {
			return 2; // Date
		}

		if (fieldno == 3) {
			return 1; // Text
		}

		if (fieldno % 3 == 0) {
			return 0; // Integer
		}

		if (fieldno % 5 == 0) {
			return 2; // Date
		}

		return 1; // Text
	}

	TestRecordBP() {
		internalDoc = new Document();
		rng = new Random();
	}

	
	TestRecordBP(POCTestOptions testOpts) {
		this(testOpts.workingset, 0, null, null);
	}

	public TestRecordBP(int workerID, int sequence, String timeStamp, Document data) {
		
		internalDoc = new Document();
		rng = new Random();

		// Always a field 0
		AddOID(workerID, sequence);

		addFields(internalDoc, 0, 3, 0, 50, 100, workerID, timeStamp, data);
	}

	public Document generateDataFields(Document doc, int seq, int nFields, int depth, int stringLength, long numberSize, int workerId) {
		Document node = new Document();
		int dataIndex = rng.nextInt(10);
		node.append("messageCode", messageCode[dataIndex]);
		node.append("action", action[dataIndex]);
		node.append("message", message[dataIndex]);
		node.append("temp", rng.nextInt(50) );
		
		return node;
	}
	
	private int addFields(Document doc, int seq, int nFields, int depth, int stringLength, long numberSize, int workerId, String timeStamp, Document data) {
		int fieldNo = seq;
		if (depth > 0) {
			// we need to create nodes not leaves
			int perLevel = (int) Math.pow(nFields, 1f / (depth + 1));
			for (int i = 0; i < perLevel; i++) {
				Document node = new Document();
				doc.append("node" + i, node);
				fieldNo += addFields(node, fieldNo, nFields / perLevel, depth - 1, stringLength, numberSize, workerId, timeStamp, data);
			}
		}
		// fields
		while (fieldNo < nFields + seq) {
			int fType = getFieldType(fieldNo);
			if (fType == 0) {
				doc.append("timestamp", timeStamp);
			} else if (fieldNo == 1 || fType == 2) // Field 2 is always a date
			// as is every 5th
			{
				Document node = new Document();
				doc.append("sensorId", "sensor-"+workerId);
			} else {
				ArrayList<Document> valueList = new ArrayList();
				valueList.add(data);
				doc.append("value", valueList);
			}
			fieldNo++;
		}
		
		return fieldNo - seq;
	}

    public List<String> listFields() {
        List<String> fields = new ArrayList<String>();
        collectFields(internalDoc, "", fields);
        return fields;
    }

    private void collectFields(Document doc, String prefix, List<String> fields) {
        Set<String> keys = doc.keySet();
        for (String key : keys) {
            if (key.startsWith("fld")) {
                fields.add(prefix + key);
            } else if (key.startsWith("node")) {
                // node
                Document node = (Document) doc.get(key);
                collectFields(node, prefix + key + ".", fields);
            }
        }
    }
    
    public static void main(String[] args) {
		TestRecordBP t = new TestRecordBP(0,0, null, null);
		logger.debug(t.toString());
	}

}
