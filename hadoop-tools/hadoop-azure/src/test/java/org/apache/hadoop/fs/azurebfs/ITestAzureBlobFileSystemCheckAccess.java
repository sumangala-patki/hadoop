/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.azurebfs;

import com.google.common.collect.Lists;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.azurebfs.constants.AbfsOperationConstants;
import org.apache.hadoop.fs.azurebfs.utils.TracingHeaderValidator;
import org.junit.Assume;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.utils.AclTestHelpers;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.security.AccessControlException;

import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ENABLE_CHECK_ACCESS;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_ID;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_SECRET;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_FS_CHECKACCESS_TEST_USER_GUID;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_FS_CLIENT_ID;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_BLOB_FS_CLIENT_SECRET;
import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT;

/**
 * Test cases for AzureBlobFileSystem.access()
 */
public class ITestAzureBlobFileSystemCheckAccess
    extends AbstractAbfsIntegrationTest {

  private static final String TEST_FOLDER_PATH = "CheckAccessTestFolder";
  private final FileSystem superUserFs;
  private FileSystem testUserFs;
  private final String testUserGuid;
  private final boolean isCheckAccessEnabled;
  private final boolean isHNSEnabled;

  public ITestAzureBlobFileSystemCheckAccess() throws Exception {
    super.setup();
    this.superUserFs = getFileSystem();
    testUserGuid = getConfiguration()
        .get(FS_AZURE_BLOB_FS_CHECKACCESS_TEST_USER_GUID);
    this.isCheckAccessEnabled = getConfiguration().isCheckAccessEnabled();
    this.isHNSEnabled = getConfiguration()
        .getBoolean(FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT, false);
  }

  private void setTestUserFs() throws Exception {
    if (this.testUserFs != null) {
      return;
    }
    String orgClientId = getConfiguration().get(FS_AZURE_BLOB_FS_CLIENT_ID);
    String orgClientSecret = getConfiguration()
        .get(FS_AZURE_BLOB_FS_CLIENT_SECRET);
    Boolean orgCreateFileSystemDurungInit = getConfiguration()
        .getBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION, true);
    getRawConfiguration().set(FS_AZURE_BLOB_FS_CLIENT_ID,
        getConfiguration().get(FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_ID));
    getRawConfiguration().set(FS_AZURE_BLOB_FS_CLIENT_SECRET, getConfiguration()
        .get(FS_AZURE_BLOB_FS_CHECKACCESS_TEST_CLIENT_SECRET));
    getRawConfiguration()
        .setBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION,
            false);
    FileSystem fs = FileSystem.newInstance(getRawConfiguration());
    getRawConfiguration().set(FS_AZURE_BLOB_FS_CLIENT_ID, orgClientId);
    getRawConfiguration().set(FS_AZURE_BLOB_FS_CLIENT_SECRET, orgClientSecret);
    getRawConfiguration()
        .setBoolean(AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION,
            orgCreateFileSystemDurungInit);
    this.testUserFs = fs;
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckAccessWithNullPath() throws IOException {
    superUserFs.access(null, FsAction.READ);
  }

  @Test(expected = NullPointerException.class)
  public void testCheckAccessForFileWithNullFsAction() throws Exception {
    assumeHNSAndCheckAccessEnabled();
    //  NPE when trying to convert null FsAction enum
    superUserFs.access(new Path("test.txt"), null);
  }

  @Test(expected = FileNotFoundException.class)
  public void testCheckAccessForNonExistentFile() throws Exception {
    assumeHNSAndCheckAccessEnabled();
    setTestUserFs();
    Path nonExistentFile = setupTestDirectoryAndUserAccess(
        "/nonExistentFile1.txt", FsAction.ALL);
    superUserFs.delete(nonExistentFile, true);
    testUserFs.access(nonExistentFile, FsAction.READ);
  }

  @Test
  public void testWhenCheckAccessConfigIsOff() throws Exception {
    Assume.assumeTrue(FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT + " is false",
        isHNSEnabled);
    Configuration conf = getRawConfiguration();
    conf.setBoolean(FS_AZURE_ENABLE_CHECK_ACCESS, false);
    FileSystem fs = FileSystem.newInstance(conf);
    Path testFilePath = setupTestDirectoryAndUserAccess("/test1.txt",
        FsAction.NONE);
    fs.access(testFilePath, FsAction.EXECUTE);
    fs.access(testFilePath, FsAction.READ);
    fs.access(testFilePath, FsAction.WRITE);
    fs.access(testFilePath, FsAction.READ_EXECUTE);
    fs.access(testFilePath, FsAction.WRITE_EXECUTE);
    fs.access(testFilePath, FsAction.READ_WRITE);
    fs.access(testFilePath, FsAction.ALL);
    testFilePath = setupTestDirectoryAndUserAccess("/test1.txt", FsAction.ALL);
    fs.access(testFilePath, FsAction.EXECUTE);
    fs.access(testFilePath, FsAction.READ);
    fs.access(testFilePath, FsAction.WRITE);
    fs.access(testFilePath, FsAction.READ_EXECUTE);
    fs.access(testFilePath, FsAction.WRITE_EXECUTE);
    fs.access(testFilePath, FsAction.READ_WRITE);
    fs.access(testFilePath, FsAction.ALL);
    fs.access(testFilePath, null);

    Path nonExistentFile = setupTestDirectoryAndUserAccess(
        "/nonExistentFile2" + ".txt", FsAction.NONE);
    superUserFs.delete(nonExistentFile, true);
    fs.access(nonExistentFile, FsAction.READ);
  }

  @Test
  public void testCheckAccessForAccountWithoutNS() throws Exception {
    Assume.assumeFalse(FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT + " is true",
        getConfiguration()
            .getBoolean(FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT, true));
    Assume.assumeTrue(FS_AZURE_ENABLE_CHECK_ACCESS + " is false",
            isCheckAccessEnabled);
    setTestUserFs();
    testUserFs.access(new Path("/"), FsAction.READ);
  }

  @Test
  public void testFsActionNONE() throws Exception {
    assumeHNSAndCheckAccessEnabled();
    setTestUserFs();
    Path testFilePath = setupTestDirectoryAndUserAccess("/test2.txt",
        FsAction.NONE);
    assertInaccessible(testFilePath, FsAction.EXECUTE);
    assertInaccessible(testFilePath, FsAction.READ);
    assertInaccessible(testFilePath, FsAction.WRITE);
    assertInaccessible(testFilePath, FsAction.READ_EXECUTE);
    assertInaccessible(testFilePath, FsAction.WRITE_EXECUTE);
    assertInaccessible(testFilePath, FsAction.READ_WRITE);
    assertInaccessible(testFilePath, FsAction.ALL);
  }

  @Test
  public void testFsActionEXECUTE() throws Exception {
    assumeHNSAndCheckAccessEnabled();
    setTestUserFs();
    Path testFilePath = setupTestDirectoryAndUserAccess("/test3.txt",
        FsAction.EXECUTE);
    AzureBlobFileSystem fs = (AzureBlobFileSystem)testUserFs;
    fs.registerListener(new TracingHeaderValidator(fs.getAbfsStore()
        .getAbfsConfiguration().getClientCorrelationID(), fs.getFileSystemID(),
        AbfsOperationConstants.ACCESS, false, 0));
    assertAccessible(testFilePath, FsAction.EXECUTE);
    fs.registerListener(null);

    assertInaccessible(testFilePath, FsAction.READ);
    assertInaccessible(testFilePath, FsAction.WRITE);
    assertInaccessible(testFilePath, FsAction.READ_EXECUTE);
    assertInaccessible(testFilePath, FsAction.WRITE_EXECUTE);
    assertInaccessible(testFilePath, FsAction.READ_WRITE);
    assertInaccessible(testFilePath, FsAction.ALL);
  }

  @Test
  public void testFsActionREAD() throws Exception {
    assumeHNSAndCheckAccessEnabled();
    setTestUserFs();
    Path testFilePath = setupTestDirectoryAndUserAccess("/test4.txt",
        FsAction.READ);
    assertAccessible(testFilePath, FsAction.READ);

    assertInaccessible(testFilePath, FsAction.EXECUTE);
    assertInaccessible(testFilePath, FsAction.WRITE);
    assertInaccessible(testFilePath, FsAction.READ_EXECUTE);
    assertInaccessible(testFilePath, FsAction.WRITE_EXECUTE);
    assertInaccessible(testFilePath, FsAction.READ_WRITE);
    assertInaccessible(testFilePath, FsAction.ALL);
  }

  @Test
  public void testFsActionWRITE() throws Exception {
    assumeHNSAndCheckAccessEnabled();
    setTestUserFs();
    Path testFilePath = setupTestDirectoryAndUserAccess("/test5.txt",
        FsAction.WRITE);
    assertAccessible(testFilePath, FsAction.WRITE);

    assertInaccessible(testFilePath, FsAction.EXECUTE);
    assertInaccessible(testFilePath, FsAction.READ);
    assertInaccessible(testFilePath, FsAction.READ_EXECUTE);
    assertInaccessible(testFilePath, FsAction.WRITE_EXECUTE);
    assertInaccessible(testFilePath, FsAction.READ_WRITE);
    assertInaccessible(testFilePath, FsAction.ALL);
  }

  @Test
  public void testFsActionREADEXECUTE() throws Exception {
    assumeHNSAndCheckAccessEnabled();
    setTestUserFs();
    Path testFilePath = setupTestDirectoryAndUserAccess("/test6.txt",
        FsAction.READ_EXECUTE);
    assertAccessible(testFilePath, FsAction.EXECUTE);
    assertAccessible(testFilePath, FsAction.READ);
    assertAccessible(testFilePath, FsAction.READ_EXECUTE);

    assertInaccessible(testFilePath, FsAction.WRITE);
    assertInaccessible(testFilePath, FsAction.WRITE_EXECUTE);
    assertInaccessible(testFilePath, FsAction.READ_WRITE);
    assertInaccessible(testFilePath, FsAction.ALL);
  }

  @Test
  public void testFsActionWRITEEXECUTE() throws Exception {
    assumeHNSAndCheckAccessEnabled();
    setTestUserFs();
    Path testFilePath = setupTestDirectoryAndUserAccess("/test7.txt",
        FsAction.WRITE_EXECUTE);
    assertAccessible(testFilePath, FsAction.EXECUTE);
    assertAccessible(testFilePath, FsAction.WRITE);
    assertAccessible(testFilePath, FsAction.WRITE_EXECUTE);

    assertInaccessible(testFilePath, FsAction.READ);
    assertInaccessible(testFilePath, FsAction.READ_EXECUTE);
    assertInaccessible(testFilePath, FsAction.READ_WRITE);
    assertInaccessible(testFilePath, FsAction.ALL);
  }

  @Test
  public void testFsActionALL() throws Exception {
    assumeHNSAndCheckAccessEnabled();
    setTestUserFs();
    Path testFilePath = setupTestDirectoryAndUserAccess("/test8.txt",
        FsAction.ALL);
    assertAccessible(testFilePath, FsAction.EXECUTE);
    assertAccessible(testFilePath, FsAction.WRITE);
    assertAccessible(testFilePath, FsAction.WRITE_EXECUTE);
    assertAccessible(testFilePath, FsAction.READ);
    assertAccessible(testFilePath, FsAction.READ_EXECUTE);
    assertAccessible(testFilePath, FsAction.READ_WRITE);
    assertAccessible(testFilePath, FsAction.ALL);
  }

  private void assumeHNSAndCheckAccessEnabled() {
    Assume.assumeTrue(FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT + " is false",
        isHNSEnabled);
    Assume.assumeTrue(FS_AZURE_ENABLE_CHECK_ACCESS + " is false",
        isCheckAccessEnabled);

    Assume.assumeNotNull(getRawConfiguration().get(FS_AZURE_BLOB_FS_CLIENT_ID));
  }

  private void assertAccessible(Path testFilePath, FsAction fsAction)
      throws IOException {
    assertTrue(
        "Should have been given access  " + fsAction + " on " + testFilePath,
        isAccessible(testUserFs, testFilePath, fsAction));
  }

  private void assertInaccessible(Path testFilePath, FsAction fsAction)
      throws IOException {
    assertFalse(
        "Should have been denied access  " + fsAction + " on " + testFilePath,
        isAccessible(testUserFs, testFilePath, fsAction));
  }

  private void setExecuteAccessForParentDirs(Path dir) throws IOException {
    dir = dir.getParent();
    while (dir != null) {
      modifyAcl(dir, testUserGuid, FsAction.EXECUTE);
      dir = dir.getParent();
    }
  }

  private void modifyAcl(Path file, String uid, FsAction fsAction)
      throws IOException {
    List<AclEntry> aclSpec = Lists.newArrayList(AclTestHelpers
        .aclEntry(AclEntryScope.ACCESS, AclEntryType.USER, uid, fsAction));
    this.superUserFs.modifyAclEntries(file, aclSpec);
  }

  private Path setupTestDirectoryAndUserAccess(String testFileName,
      FsAction fsAction) throws Exception {
    Path file = new Path(TEST_FOLDER_PATH + testFileName);
    file = this.superUserFs.makeQualified(file);
    this.superUserFs.delete(file, true);
    this.superUserFs.create(file);
    modifyAcl(file, testUserGuid, fsAction);
    setExecuteAccessForParentDirs(file);
    return file;
  }

  private boolean isAccessible(FileSystem fs, Path path, FsAction fsAction)
      throws IOException {
    try {
      fs.access(path, fsAction);
    } catch (AccessControlException ace) {
      return false;
    }
    return true;
  }
}
