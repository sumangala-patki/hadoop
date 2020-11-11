package org.apache.hadoop.fs.azurebfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.azurebfs.constants.HttpHeaderConfigurations;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpHeader;
import org.apache.hadoop.fs.azurebfs.services.AbfsHttpOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperationType;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;
import org.apache.http.protocol.HTTP;
import org.assertj.core.api.Assertions;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants.HTTP_METHOD_PUT;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_MAX_IO_RETRIES;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_CLIENT_CORRELATIONID;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MockRestOperation extends AbfsRestOperation {
  int prevRetryCount = 0;
  String prevClientRequestId = "";
  private final URL url;
  private final List<AbfsHttpHeader> requestHeaders;

  MockRestOperation(AbfsRestOperationType type,
      AbfsClient client,
      String method,
      URL url,
      List<AbfsHttpHeader> requestHeaders) {
    super(type, client, method, url, requestHeaders, null);
    this.url = url;
    this.requestHeaders = requestHeaders;
  }
  @Override
  protected boolean executeHttpOperation(final int retryCount,
      TracingContext tracingContext) {
    try {
      updateClientRequestHeader(new AbfsHttpOperation(url, HTTP_METHOD_PUT, requestHeaders),
          tracingContext);
    } catch (IOException e) {
    }
    testRetryNumber(retryCount, tracingContext);
    return false;
  }

  public void testRetryNumber(int retryCount, TracingContext tracingContext) {
    tracingContext.setListener(null);
    String[] id_list = tracingContext.toString().split(":");
    String clientRequestId = id_list[1];
    int retryCountTC = Integer.parseInt(id_list[6]);

    // check if retryCount correctly copied to tracingContext
    Assertions.assertThat(retryCountTC)
        .describedAs("Retry count not updated correctly")
        .isEqualTo(retryCount);

    if(retryCount > 0) {
      //check if retryCount is incremented for retried request
      Assertions.assertThat(retryCountTC)
          .describedAs("Retry count should be updated")
          .isEqualTo(prevRetryCount + 1);
      prevRetryCount = retryCount;

      //check that clientRequestId is uniquely generated for retried request
      Assertions.assertThat(clientRequestId)
          .describedAs("client-req-id should be unique for retried requests")
          .isNotEqualTo(prevClientRequestId);
      prevClientRequestId = clientRequestId;
    }
  }
}

public class ITestCorrelationHeader extends AbstractAbfsIntegrationTest {
  private static final String[] CLIENT_CORRELATIONID_LIST = {
      "valid-corr-id-123", "inval!d", ""};
  private static final int HTTP_CREATED = 201;

  public ITestCorrelationHeader() throws Exception {
    super();
  }

  @Test
  //check if clientCorrelationId from config is validated and stored correctly
  public void testClientCorrelationID() throws IOException {
    checkCorrelationConfigValidation(CLIENT_CORRELATIONID_LIST[0], true);
    checkCorrelationConfigValidation(CLIENT_CORRELATIONID_LIST[1], false);
    checkCorrelationConfigValidation(CLIENT_CORRELATIONID_LIST[2], false);
  }

  public void checkCorrelationConfigValidation(String clientCorrelationId, boolean includeInHeader)
      throws IOException {
    Configuration config = new Configuration(this.getRawConfiguration());
    config.set(FS_AZURE_CLIENT_CORRELATIONID, clientCorrelationId);

    final AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem
        .newInstance(this.getFileSystem().getUri(), config);

    AbfsRestOperation op = fs.getAbfsClient().createPath("/testDir",
        false, true, null, null, false,
        null, getTestTracingContext(fs, false));

    int responseCode = op.getResult().getStatusCode();
    Assertions.assertThat(responseCode).describedAs("Status code")
        .isEqualTo(HTTP_CREATED);

    String requestHeader = op.getResult()
        .getRequestHeader(HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID);
    String headerCorrelationID = requestHeader.replace("[", "")
        .replace("]", "").split(":")[0];
    if (includeInHeader) {
      Assertions.assertThat(headerCorrelationID).describedAs(
          "Header should contain correlationId as per config")
          .isEqualTo(clientCorrelationId);
    } else {
      Assertions.assertThat(headerCorrelationID).describedAs(
          "Invalid correlationId value should not be included in header")
          .isEmpty();
    }
  }

  @Test
  //check whether IDs in tracingContext are reflected in requestHeader
  public void testRequestHeader() throws IOException {
    AzureBlobFileSystem fs = createFileSystem();
    String testCorrelationId =
        fs.getAbfsStore().getAbfsConfiguration().getClientCorrelationID();
    String testOpName = "AB";
    TracingContext tracingContext = new TracingContext(testCorrelationId,
        fs.getFileSystemID(), testOpName, true, 1, null);
    tracingContext.generateClientRequestID();
    tracingContext.setStreamID("test-stream-id");

    AbfsRestOperation op = fs.getAbfsClient().createPath("/testDir",
        false, true, null, null, false,
        null, tracingContext);

    String requestHeader =
        op.getResult().getRequestHeader(HttpHeaderConfigurations.X_MS_CLIENT_REQUEST_ID)
            .replace("[", "").replace("]", "");

    Assertions.assertThat(requestHeader)
        .describedAs("IDs in request header should match tracingContext")
        .isEqualTo(tracingContext.toString());
  }

  @Test
  //check if retried requests have unique clientRequestId and updated retryCount
  public void testRetryCount() throws IOException {
    Configuration config = new Configuration(this.getRawConfiguration());
    config.set(AZURE_MAX_IO_RETRIES, Integer.toString(4));
    AzureBlobFileSystem fs =
        (AzureBlobFileSystem) FileSystem.newInstance(getFileSystem().getUri()
            , config);
    TracingContext tracingContext = getTestTracingContext(fs, false);
//    AbfsRestOperation op =
//        new MockRestOperation(AbfsRestOperationType.CreatePath,
//            fs.getAbfsClient(), HTTP_METHOD_PUT,
//            new URL("http://www.azure.com"), new ArrayList<>());
//    op.execute(tracingContext);

    AbfsRestOperation oprest = mock(AbfsRestOperation.class);
//    doThrow(IOException.class).when(oprest)
//        .updateClientRequestHeader(any(AbfsHttpOperation.class),
//        any(TracingContext.class));
    TracingContext tracingContext1 = mock(TracingContext.class);
    doThrow(IOException.class).when(tracingContext1)
        .generateClientRequestID();
    oprest.execute(tracingContext1);
  }

  @Ignore
  @Test
  //call test methods from the respective test classes
  //can be ignored when running all tests as these get covered
  public void runCorrelationTestForAllMethods() throws Exception {
    //map to avoid creating new instance and calling setup() for each test
    Map<AbstractAbfsIntegrationTest, Method> testClasses = new HashMap<>();

    testClasses.put(new ITestAzureBlobFileSystemListStatus(),
        ITestAzureBlobFileSystemListStatus.class.getMethod("testListPath"));
    testClasses.put(new ITestAzureBlobFileSystemCreate(),
        ITestAzureBlobFileSystemCreate.class.getMethod(
            "testDefaultCreateOverwriteFileTest"));
    //add other ops' testClasses and testMethods that have listener registered

    for (AbstractAbfsIntegrationTest testClass : testClasses.keySet()) {
      testClass.setup();
      testClasses.get(testClass).invoke(testClass);
      testClass.teardown();
    }
  }
}
