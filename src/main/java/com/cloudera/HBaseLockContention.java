package com.cloudera;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Hello world!
 */
public class HBaseLockContention {

  public static final int NUM_HANDLERS = 50;

  public static void main(String[] args)
      throws ZooKeeperConnectionException, MasterNotRunningException, InterruptedException {
    String tableName = "TestLockContention";

    Preparer p = new Preparer(tableName);
    p.run();

    ExecutorService e = Executors.newFixedThreadPool(NUM_HANDLERS);
    List<Inserter> inserters = new ArrayList<Inserter>();
    for (int j = 0; j < 10; j++) {
      inserters.add(new Inserter(tableName));
    }
    e.invokeAll(inserters);

    List<TableScanner> scanners = new ArrayList<TableScanner>();
    for(int i =0; i < NUM_HANDLERS * 200; i++) {
      scanners.add(new TableScanner(tableName));

    }

    e.invokeAll(scanners);
  }
}
