package com.cloudera;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;

/**
 *
 */
public class Preparer implements Runnable{
  HTable table;
  HBaseAdmin admin;
  private String tableName;
  private final Configuration c;



  public Preparer(String tableName) throws
      ZooKeeperConnectionException,
      MasterNotRunningException {
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
  }
}
