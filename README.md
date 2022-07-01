# Introduction

Disclaimer: POCTimeseries is NOT in any way an official MongoDB product or project.

This is open source, immature, and undoubtedly buggy code. If you find bugs please fix them and send a pull request or report in the GitHub issue queue.

This tool is designed to make it easy to answer many of the questions people have during a MongoDB 'Proof of Concept':

How fast will MongoDB Time series collection be on my hardware?
How could MongoDB handle my workload in Time series collections?
What are the performance difference between Time series collections and bucket patterns.

POCTimeseries is a single JAR file which allows you to specify and run a number of different workloads easily from the command line. It is intended to show how MongoDB Timeseries collections should be used for various tasks and comparing them with bucket patterns.

# Build

# Execute

$ mvn clean package
and you will find POCDriver.jar in bin folder.


# Client options
  -t = number of threads
  -d = duration of test
  -c = connection string
  -tscoll = is time series collection

# Example
$ java -jar POCDriver.jar   -t 100 -d 3600   -c <connection_string> -tscoll true

# Requirements to Build

commons-cli-1.3.jar
commons-codec-1.10.jar
gson-2.2.4.jar
loremipsum-1.0.jar (http://sourceforge.net/projects/loremipsum/files/)
mongo-driver-sync-3.8.1.jar

# Troubleshooting

If you are running a mongod with --auth enabled, you must pass a user and password with read/write and replSetGetStatus privileges (e.g. readWriteAnyDatabase and clusterMonitor roles).
