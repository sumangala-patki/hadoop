package org.apache.hadoop.fs.azurebfs.utils;

public interface Listener {
  void afterOp(String header);
}
