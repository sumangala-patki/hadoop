package org.apache.hadoop.fs.azurebfs.utils;

import org.apache.hadoop.fs.azurebfs.constants.AbfsOperations;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations;
import org.assertj.core.api.Assertions;

public class TestHeader implements Listener {
  String prevContinuationHeader = "";
  int prevRetryCount = 0;
  String clientCorrelationID;
  String fileSystemID;
  String streamID = "";
  String operation;
  int maxRetryCount = FileSystemConfigurations.DEFAULT_MAX_RETRY_ATTEMPTS;
  String GUID_PATTERN = "[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}";
  // client-req-id as per docs: ^[{(]?[0-9a-f]{8}[-]?([0-9a-f]{4}[-]?)
  // {3}[0-9a-f]{12}[)}]?$

  @Override
  public void afterOp(String header) {
    System.out.println("testing header: " + header);
    testBasicFormat(header);

    if (isContinuationOp(header)) {
      // assert empty if not? pReqId is not empty for some ops not in contn list
      checkContinuationConsistency(header);
      prevContinuationHeader = header;
    }
  }

  public TestHeader(String clientCorrelationID,
      String fileSystemID, String operation, int maxRetryCount) {
    this.clientCorrelationID = clientCorrelationID;
    this.fileSystemID = fileSystemID;
    this.operation = operation;
    this.maxRetryCount = maxRetryCount;
  }

  /*
  public TestHeader(String streamID, String streamHeader) {
    String[] id_list = streamHeader.split(":");
    clientCorrelationID = id_list[0];
    fileSystemID = id_list[2];
    operation = id_list[5];
    this.streamID = streamID;
  }
  */

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

  private boolean isStreamOperation(String header) {
    String op = header.split(":")[5];
    switch (op) {
    case AbfsOperations.APPEND:
    case AbfsOperations.FLUSH:
    case AbfsOperations.READ: return true;
    }
    return false;
  }

  private void checkContinuationConsistency(String newHeader) {
    String[] newIDs = newHeader.split(":");
    Assertions.assertThat(newIDs[3])
        .describedAs("Continuation ops should have primary request id")
        .isNotEmpty();
    if (prevContinuationHeader.isEmpty())
      return;
    String[] prevIDs = prevContinuationHeader.split(":");
    Assertions.assertThat(newIDs[2])
        .describedAs("FilesystemID should be same for requests with same filesystem")
        .isEqualTo(prevIDs[2]);
    Assertions.assertThat (newIDs[3])
        .describedAs("PrimaryReqID should be same for given set of requests")
        .isEqualTo(prevIDs[3]);
    System.out.println("all tests done");
  }

  private void testBasicFormat(String header) {
    String[] id_list = header.split(":");
    Assertions.assertThat(id_list)
        .describedAs("header should have 7 elements").hasSize(7);

    //validate values
    Assertions.assertThat(id_list[0])
        .describedAs("Correlation ID should match config")
        .isEqualTo(clientCorrelationID);
    Assertions.assertThat(id_list[1])
        .describedAs("Client request ID is a guid")
        .matches(GUID_PATTERN);
    Assertions.assertThat(id_list[2]).describedAs("Filesystem ID incorrect")
        .isEqualTo(fileSystemID);
    if (!isStreamOperation(header)) {
      Assertions.assertThat(id_list[4])
          .describedAs("StreamId should be empty").isEmpty();
    } else {
      Assertions.assertThat(id_list[4])
          .describedAs("StreamId should not be empty").isNotEmpty();
    }
    Assertions.assertThat(id_list[5]).describedAs("Operation name incorrect")
        .isEqualTo(operation);
    int retryCount = Integer.parseInt(id_list[6]);
    Assertions.assertThat(retryCount)
        .describedAs("Retry count is not within range")
        .isLessThan(maxRetryCount).isGreaterThanOrEqualTo(0);
    if(retryCount > 0) {
      Assertions.assertThat(retryCount)
          .describedAs("Retry count incorrect")
          .isEqualTo(prevRetryCount + 1);
    }
    prevRetryCount = retryCount;
    System.out.println("basic test done");
  }
}
