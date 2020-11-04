package org.apache.hadoop.fs.azurebfs.utils;

import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem;
import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.constants.AbfsOperations;
import org.apache.hadoop.fs.azurebfs.constants.FileSystemConfigurations;
import org.assertj.core.api.Assertions;

public class testHeader implements Listener {
  String prevContinuationHeader = "";
  String clientCorrelationID;
  String fileSystemID;
  String streamID = "";
  String operation;
  int maxRetryCount = FileSystemConfigurations.DEFAULT_MAX_RETRY_ATTEMPTS;
  String GUID_PATTERN = "[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}";

  @Override
  public void afterOp(String header) {
    System.out.println("testing header: " + header);
    testBasicFormat(header);

    if (isContinuationOp(header)) {
      checkContinuationConsistency(header);
      prevContinuationHeader = header;
    }
  }

  public testHeader(String clientCorrelationID,
      String fileSystemID, String operation, int maxRetryCount) {
    this.clientCorrelationID = clientCorrelationID;
    this.fileSystemID = fileSystemID;
    this.operation = operation;
    this.maxRetryCount = maxRetryCount;
  }

  public testHeader(String streamID, String streamHeader) {
    String[] id_list = streamHeader.split(":");
    clientCorrelationID = id_list[0];
    fileSystemID = id_list[2];
    operation = id_list[5];
    this.streamID = streamID;
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
    Assertions.assertThat(newIDs[3])
        .describedAs("Continuation ops should have primary request id")
        .isNotEmpty();
    if (prevContinuationHeader.isEmpty())
      return;
    String[] prevIDs = prevContinuationHeader.split(":");
    Assertions.assertThat(newIDs[2])
        .describedAs("FilesystemID should be same for requests with same filesystem")
        .isEqualTo(prevIDs[2]);
    Assertions.assertThat(newIDs[0])
        .describedAs("CorrelationID should be same for requests using a given config")
        .isEqualTo(prevIDs[0]);
    Assertions.assertThat (newIDs[3])
        .describedAs("PrimaryReqID should be same for given set of requests")
        .isEqualTo(prevIDs[3]);
    Assertions.assertThat(newIDs[4])
        .describedAs("StreamID should be same for a given stream")
        .isEqualTo(prevIDs[4]);
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
    Assertions.assertThat(id_list[5]).describedAs("Operation name incorrect")
        .isEqualTo(operation);
    Assertions.assertThat(Integer.parseInt(id_list[6]))
        .describedAs("Retry count is not within range")
        .isLessThan(maxRetryCount).isGreaterThanOrEqualTo(0);
    System.out.println("basic test done");
  }
}
