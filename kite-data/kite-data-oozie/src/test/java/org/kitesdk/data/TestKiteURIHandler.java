/**
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
package org.kitesdk.data;

import java.net.URI;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.data.oozie.KiteURIHandler;
import org.kitesdk.data.spi.filesystem.FileSystemDatasetRepository;

import com.google.common.io.Files;

public class TestKiteURIHandler {
  private FileSystemDatasetRepository repo;
  private KiteURIHandler uriHandler;

  @Before
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fileSystem = FileSystem.get(conf);
    Path testDirectory = fileSystem.makeQualified(
        new Path(Files.createTempDir().getAbsolutePath()));
    this.repo = new FileSystemDatasetRepository.Builder().configuration(conf)
        .rootDirectory(testDirectory).build();
    uriHandler = new KiteURIHandler();
  }
  
  @Test
  public void testUriExists() throws Exception {
    Dataset<Object> dataset = repo.create("test", new DatasetDescriptor.Builder().schema(Schema.create(Schema.Type.STRING)).build());
    URI datasetUri = URI.create(repo.getUri() + "?dataset-name=test");
    Assert.assertFalse(uriHandler.exists(datasetUri, null));
    
    dataset.freeze();
    Assert.assertTrue(uriHandler.exists(datasetUri, null));
  }
}
