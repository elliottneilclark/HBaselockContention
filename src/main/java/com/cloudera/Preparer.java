package com.cloudera;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Preparer implements Runnable {

  HTable table;
  HBaseAdmin admin;
  private String tableName;
  private final Configuration c;

  public Preparer(String tableName) throws ZooKeeperConnectionException, MasterNotRunningException {
    this.tableName = tableName;
    c = HBaseConfiguration.create();
    admin = new HBaseAdmin(c);

  }

  @Override
  public void run() {
    try {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);

    } catch (IOException e) {

    }

    HTableDescriptor descriptor = new HTableDescriptor(tableName);
    HColumnDescriptor family = new HColumnDescriptor("d");
    family.setMaxVersions(1);
    family.setTimeToLive(1);
    family.setMinVersions(1);
    descriptor.addFamily(family);

    try {
      admin.createTable(descriptor);
    } catch (IOException e) {
      e.printStackTrace();
    }

    try {
      table = new HTable(c, tableName);
      table.setAutoFlush(false);
      for (long i = 0; i < 1000000; i++) {
        byte[] rk = Bytes.toBytes(i);
        ArrayUtils.reverse(rk);
        Put p = new Put(rk);
        p.add(Bytes.toBytes("d"), Bytes.toBytes("One"), Bytes.toBytes(i));
        p.add(Bytes.toBytes("d"),
            Bytes.toBytes("Two"),
            Bytes.toBytes(RandomStringUtils.randomAlphabetic(5)));
        table.put(p);
      }
      table.flushCommits();
      admin.flush(tableName);
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } catch (InterruptedException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }
}
