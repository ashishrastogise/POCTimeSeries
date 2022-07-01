package com.monogdb.poctimeseries;


import java.util.logging.LogManager;

import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class POCTimeseries {

    public static void main(String[] args) {

        POCTestOptions testOpts;
        LogManager.getLogManager().reset();
        Logger logger = LoggerFactory.getLogger(POCTimeseries.class);

        logger.info("MongoDB Proof Of Concept  - Time Series Load Generator");
        try {
            testOpts = new POCTestOptions(args);
            // Quit after displaying help message
            if (testOpts.helpOnly) {
                return;
            }

        } catch (ParseException e) {
            System.err.println(e.getMessage());
            return;
        }

        POCTestResults testResults = new POCTestResults(testOpts);
        LoadRunner runner = new LoadRunner(testOpts);
        runner.runLoad(testOpts, testResults);
    }

}
