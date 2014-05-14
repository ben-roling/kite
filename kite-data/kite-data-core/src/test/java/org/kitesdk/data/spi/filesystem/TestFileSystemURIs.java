/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data.spi.filesystem;

import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetRepositories;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.DatasetRepositoryException;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.MiniDFSTest;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.Datasets.LoadFlag;
import org.kitesdk.data.spi.MetadataProvider;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.BeforeClass;

import com.google.common.io.Files;

import static org.kitesdk.data.spi.filesystem.DatasetTestUtilities.*;

public class TestFileSystemURIs extends MiniDFSTest {

  @BeforeClass
  public static void loadImpl() {
    new Loader().load();
  }
  
  private FileSystem fileSystem = getFS();
  private Path testDirectory;
  
  @Before
  public void setUp() throws IOException {
    testDirectory = fileSystem.makeQualified(
        new Path(Files.createTempDir().getAbsolutePath()));
  }

  @After
  public void tearDown() throws IOException {
    fileSystem.delete(testDirectory, true);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testLocalRelative() throws URISyntaxException {
    URI repositoryUri = new URI("repo:file:target/dsr-repo-test");
    DatasetRepository repository = DatasetRepositories.open(repositoryUri);

    // We only do the deeper implementation checks one per combination.
    Assert.assertNotNull("Received a repository", repository);
    Assert.assertTrue("Repo is a FileSystem repo",
        repository instanceof FileSystemDatasetRepository);
    MetadataProvider provider = ((FileSystemDatasetRepository) repository)
        .getMetadataProvider();
    Assert.assertTrue("Repo is using a FileSystemMetadataProvider",
        provider instanceof FileSystemMetadataProvider);
    FileSystemMetadataProvider fsProvider = (FileSystemMetadataProvider) provider;
    Assert.assertTrue("FileSystem is a LocalFileSystem",
        fsProvider.getFileSytem() instanceof LocalFileSystem);
    Path expected = fsProvider.getFileSytem().makeQualified(
        new Path("target/dsr-repo-test"));
    Assert.assertEquals("Root directory should be the correct qualified path",
        expected, fsProvider.getRootDirectory());
    Assert.assertEquals("Repository URI scheme", "repo", repository.getUri()
        .getScheme());
    Assert.assertEquals("Repository URI scheme", expected.toUri(),
        new URI(repository.getUri().getSchemeSpecificPart()));
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testLocalAbsolute() throws URISyntaxException {
    URI repositoryUri = new URI("repo:file:///tmp/dsr-repo-test");
    DatasetRepository repository = DatasetRepositories.open(repositoryUri);

    FileSystemMetadataProvider provider = (FileSystemMetadataProvider)
        ((FileSystemDatasetRepository) repository).getMetadataProvider();
    Assert.assertEquals("Root directory should be the correct qualified path",
        new Path("file:/tmp/dsr-repo-test"), provider.getRootDirectory());
    Assert.assertEquals("Repository URI", repositoryUri, repository.getUri());
  }

  @Test(expected = DatasetRepositoryException.class)
  public void testHdfsFailsDefault() {
    // the environment doesn't contain the HDFS URI, so this should cause a
    // DatasetRepository exception about not finding HDFS
    DatasetRepositories.open("repo:hdfs:/");
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testHdfsAbsolute() throws URISyntaxException {
    URI hdfsUri = getDFS().getUri();
    URI repositoryUri = new URI("repo:hdfs://" + hdfsUri.getAuthority() + "/tmp/dsr-repo-test");
    DatasetRepository repository = DatasetRepositories.open(repositoryUri);

    // We only do the deeper implementation checks one per combination.
    Assert.assertNotNull("Received a repository", repository);
    Assert.assertTrue("Repo is a FileSystem repo",
        repository instanceof FileSystemDatasetRepository);
    MetadataProvider provider = ((FileSystemDatasetRepository) repository)
        .getMetadataProvider();
    Assert.assertTrue("Repo is using a FileSystemMetadataProvider",
        provider instanceof FileSystemMetadataProvider);
    FileSystemMetadataProvider fsProvider = (FileSystemMetadataProvider) provider;
    Assert.assertTrue("FileSystem is a DistributedFileSystem",
      fsProvider.getFileSytem() instanceof DistributedFileSystem);
    Path expected = fsProvider.getFileSytem().makeQualified(
        new Path("/tmp/dsr-repo-test"));
    Assert.assertEquals("Root directory should be the correct qualified path",
        expected, fsProvider.getRootDirectory());
    Assert.assertEquals("Repository URI", repositoryUri, repository.getUri());
  }
  
  @Test
  public void testLoadDatasetDoesntExist() {
    Dataset<Object> ds = Datasets.load("repo:" + getFS().makeQualified(testDirectory) + "?dataset-name=test",
        LoadFlag.BASE_DATASET_ONLY);
    Assert.assertNull(ds);
  }
  
  @Test
  public void testLoadDatasetExists() {
    String repoUri = "repo:" + getFS().makeQualified(testDirectory);
    DatasetRepository repo = DatasetRepositories.open(repoUri);
    repo.create("test", new DatasetDescriptor.Builder().schema(DatasetTestUtilities.USER_SCHEMA).build());
    
    Dataset<Object> ds = Datasets.load(repoUri + "?dataset-name=test", LoadFlag.BASE_DATASET_ONLY);
    Assert.assertEquals("test", ds.getName());
    Assert.assertEquals(USER_SCHEMA, ds.getDescriptor().getSchema());
  }
  
  @Test
  public void testLoadPartitionDoesntExistNoAutoCreate() {
    String repoUri = "repo:" + getFS().makeQualified(testDirectory);
    DatasetRepository repo = DatasetRepositories.open(repoUri);
    repo.create(
        "test",
        new DatasetDescriptor.Builder()
            .schema(DatasetTestUtilities.USER_SCHEMA)
            .partitionStrategy(
                new PartitionStrategy.Builder().hash("username", 5)
                    .hash("email", 10).build()).build());
    
    Dataset<Object> ds = Datasets.load(repoUri + "?dataset-name=test&partition-key=[1,6]");
    Assert.assertNull(ds);
  }
  
  @Test
  public void testLoadPartitionDoesntExistAutoCreate() {
    String repoUri = "repo:" + getFS().makeQualified(testDirectory);
    DatasetRepository repo = DatasetRepositories.open(repoUri);
    repo.create(
        "test",
        new DatasetDescriptor.Builder()
            .schema(DatasetTestUtilities.USER_SCHEMA)
            .partitionStrategy(
                new PartitionStrategy.Builder().hash("username", 5)
                    .hash("email", 10).build()).build());
    
    Dataset<Object> ds = Datasets.load(repoUri + "?dataset-name=test&partition-key=[1,6]",
        LoadFlag.AUTO_CREATE_PARTITION);
    Assert.assertEquals("test", ds.getName());
    Assert.assertEquals(USER_SCHEMA, ds.getDescriptor().getSchema());
    
    // its a leaf partition so the partition itself is not partitioned
    Assert.assertFalse(ds.getDescriptor().isPartitioned());
  }
  
  @Test
  public void testLoadPartitionExists() {
    String repoUri = "repo:" + getFS().makeQualified(testDirectory);
    DatasetRepository repo = DatasetRepositories.open(repoUri);
    PartitionStrategy partitionStrategy = new PartitionStrategy.Builder().hash("username", 5)
        .hash("email", 10).build();
    Dataset<Object> ds = repo.create(
        "test",
        new DatasetDescriptor.Builder()
            .schema(DatasetTestUtilities.USER_SCHEMA)
            .partitionStrategy(
                partitionStrategy).build());
    
    ds.getPartition(partitionStrategy.partitionKey(1, 6), true);
    Dataset<Object> partition = Datasets.load(repoUri + "?dataset-name=test&partition-key=[1,6]");
    
    Assert.assertEquals("test", partition.getName());
    Assert.assertEquals(USER_SCHEMA, partition.getDescriptor().getSchema());
    
    // its a leaf partition so the partition itself is not partitioned
    Assert.assertFalse(partition.getDescriptor().isPartitioned());
  }

}
