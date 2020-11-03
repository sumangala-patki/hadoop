package org.apache.hadoop.fs.azurebfs.utils;

public class testHeader implements Listener{
  @Override
  public void afterOp(String header) {
    //test header
    System.out.println("testing header: " + header);
  }
}
