package com.cloudera;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;

import javax.swing.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Hello world!
 */
public class HBaseLockContention {

  public static final int NUM_HANDLERS = 30;

  public static void main(String[] args)
      throws ZooKeeperConnectionException, MasterNotRunningException, InterruptedException {
    String tableName = "TestLockContention";
    Preparer preparer = new Preparer(tableName);
    preparer.run();

    ExecutorService e = Executors.newFixedThreadPool(NUM_HANDLERS);
    List<TableScanner> scanners = new ArrayList<TableScanner>();
    for(int i =0; i < NUM_HANDLERS * 10; i++) {
      scanners.add(new TableScanner(tableName));

    }

    e.invokeAll(scanners);
  }
}
