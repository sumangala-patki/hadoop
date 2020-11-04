package org.apache.hadoop.fs.azurebfs.utils;

import org.apache.hadoop.fs.azurebfs.constants.AbfsOperations;
import org.assertj.core.api.Assertions;

public class testHeader implements Listener {
  String prevContinuationHeader = "";

  @Override
  public void afterOp(String header) {
    System.out.println("testing header: " + header);
    testBasicFormat(header);

    if (isContinuationOp(header)) {
      checkContinuationConsistency(header);
      prevContinuationHeader = header;
    }
  }

  private boolean isContinuationOp(String header) {
    String op = header.split(":")[5];
    switch (op) {
    case AbfsOperations.LISTSTATUS:
    case AbfsOperations.READ:
    case AbfsOperations.RENAME:
    case AbfsOperations.DELETE:
    case AbfsOperations.CREATE: return true;
    }
    return false;
  }

  private void checkContinuationConsistency(String newHeader) {
    String[] newIDs = newHeader.split(":");
    Assertions.assertThat(newIDs[3]).describedAs("Continuation ops should have "
        + "a primary request id")
        .isNotEmpty();
    if (prevContinuationHeader.isEmpty())
      return;
    String[] prevIDs = prevContinuationHeader.split(":");
    Assertions.assertThat(newIDs[2])
        .describedAs("filesystem id should be same for requests with same filesystem")
        .isEqualTo(prevIDs[2]);
    Assertions.assertThat(newIDs[0])
        .describedAs("correlation id should be same for requests using a given config")
        .isEqualTo(prevIDs[0]);
    Assertions.assertThat (newIDs[3])
        .describedAs("preq id should be same for given set of requests")
        .isEqualTo(prevIDs[3]);
    Assertions.assertThat(newIDs[4])
        .describedAs("stream id should be same for a given stream")
        .isEqualTo(prevIDs[4]);
  }

  private void testBasicFormat(String header) {
    String[] id_list = header.split(":");
    Assertions.assertThat(id_list)
        .describedAs("header should have 7 elements").hasSize(7);
    //check that necessary ids are not empty
    Assertions.assertThat(id_list[1]).isNotEmpty();  //client request id
    Assertions.assertThat(id_list[2]).isNotEmpty();  //filesystem id
    Assertions.assertThat(Integer.parseInt(id_list[6]))
        .describedAs("Retry count should be between 0 and 30")
        .isLessThan(30).isGreaterThanOrEqualTo(0);
  }
}
