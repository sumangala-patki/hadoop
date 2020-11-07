package org.apache.hadoop.fs.azurebfs.utils;

import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;

public interface Listener {
  void afterOp(String header);
}
