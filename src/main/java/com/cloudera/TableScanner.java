package com.cloudera;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.concurrent.Callable;

public class TableScanner implements Callable<String> {

  private String tableName;

  public TableScanner(String tableName) {

    this.tableName = tableName;
  }

  @Override
  public String call() throws Exception {
    HTable table = new HTable(HBaseConfiguration.create(), tableName);

    Scan s = new Scan();
    s.setCaching(10);
    for (int i = 0; i< 10000; i++) {
      ResultScanner rs = table.getScanner(s);

      for (Result r = rs.next(); r != null; r = rs.next()) {
        //Totally ignored
      }
    }
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
