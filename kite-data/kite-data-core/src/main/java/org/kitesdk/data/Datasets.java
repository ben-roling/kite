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
package org.kitesdk.data;

import java.net.URI;
import java.util.Map;

import org.kitesdk.data.spi.AbstractDataset;
import org.kitesdk.data.spi.AbstractDatasetRepository;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;

/**
 * <p>Convenience methods for working with {@link Dataset} instances.</p> 
 */
public class Datasets {
  private static final String DATASET_NAME_PARAM = "dataset-name";
  private static final String PARTITION_KEY_PARAM = "partition-key";

  private static final Splitter.MapSplitter QUERY_SPLITTER =
      Splitter.on('&').withKeyValueSeparator(Splitter.on('='));

  /**
   * Loads a Dataset from a datasetUri.
   * @param datasetUri non-null datasetUri
   * @param autoCreatePartition true to create the partition if it doesn't exist in the case
   * that the URI refers to a partition
   * @return the Dataset or null if the URI refers to a dataset that does not exist.
   * @throws DatasetException
   */
  public static <E> Dataset<E> load(String datasetUri, boolean autoCreatePartition) {
    DatasetRepository repository = DatasetRepositories.open(datasetUri);
    
    URI storageUri = DatasetRepositories.getStorageUri(datasetUri);
    String query = storageUri.getQuery();
    Preconditions.checkNotNull(query, "URI is not a dataset URI: " + datasetUri);
    
    Map<String, String> queryParams = QUERY_SPLITTER.split(query);
    String datasetName = queryParams.get(DATASET_NAME_PARAM);
    Preconditions.checkNotNull(datasetName, "URI is not a dataset URI: " + datasetUri);
    if (!repository.exists(datasetName)) {
      return null;
    }
    
    Dataset<E> baseDataset = repository.load(datasetName);
    String partitionKey = queryParams.get(PARTITION_KEY_PARAM);
    if (partitionKey != null) {
      AbstractDataset<E> abstractDataset = (AbstractDataset<E>) baseDataset;
      return baseDataset.getPartition(abstractDataset.toPartitionKey(partitionKey), autoCreatePartition);
    }
    
    return baseDataset;
  }
  
  static <E> String getUri(Dataset<E> dataset, String partitionKey) {
    String repositoryUriString = dataset.getDescriptor().getProperty(
        AbstractDatasetRepository.REPOSITORY_URI_PROPERTY_NAME);
    if (repositoryUriString == null) {
      // Datasets outside a repository have no URI
      return null;
    }
    
    URI repositoryUri = URI.create(repositoryUriString);

    String datasetUri = String.format("%s%sdataset-name=%s",repositoryUriString,
        repositoryUri.getQuery() == null ? "?" : "&", dataset.getName());
    if (partitionKey != null) {
      return datasetUri + "&partition-key=" + partitionKey;
    }
    
    return datasetUri;
  }
  
}
