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
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import javax.annotation.Nullable;

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
   * Flags used to affect behavior when loading a Dataset from a URI 
   */
  public enum LoadFlag {
    /**
     * Load only the base Dataset even when given a partition or view URI 
     */
    BASE_DATASET_ONLY,
    /**
     * Create the partition when given a partition URI identifying a partition that doesn't yet exist
     */
    AUTO_CREATE_PARTITION
  }
  
  /**
   * Loads a Dataset from a URI
   * 
   * @param datasetUri the URI identifying the dataset.  URIs may identify base Datasets or partitions
   * within the Dataset.
   * @param loadFlag flags to affect load behavior
   * @return the Dataset or null when referring to a partition that does not yet exist in absence
   * of {@link LoadFlag#AUTO_CREATE_PARTITION}
   * @throws DatasetException
   */
  public static <E> Dataset<E> load(String datasetUri, LoadFlag... loadFlag) {
    DatasetRepository repository = DatasetRepositories.open(datasetUri);
    
    Map<String, String> queryParams = getQueryParams(datasetUri);
    String datasetName = queryParams.get(DATASET_NAME_PARAM);
    Preconditions.checkNotNull(datasetName, "URI is not a dataset URI: " + datasetUri);
    if (!repository.exists(datasetName)) {
      return null;
    }
    
    Dataset<E> baseDataset = repository.load(datasetName);
    Collection<LoadFlag> loadFlags = Arrays.asList(loadFlag);
    if (loadFlags.contains(LoadFlag.BASE_DATASET_ONLY)) {
      return baseDataset;
    }
    
    String partitionKeyString = queryParams.get(PARTITION_KEY_PARAM);
    if (partitionKeyString != null) {
      return baseDataset.getPartition(getPartitionKey(baseDataset, partitionKeyString),
          loadFlags.contains(LoadFlag.AUTO_CREATE_PARTITION));
    }
    
    return baseDataset;
  }

  /**
   * Equivalent to {@link #load(String, LoadFlag...)} with datasetUri.toString()
   */
  public static <E> Dataset<E> load(URI datasetUri, LoadFlag... loadFlag) {
    return load(datasetUri.toString(), loadFlag);
  }

  private static Map<String, String> getQueryParams(String datasetUri) {
    URI storageUri = DatasetRepositories.getStorageUri(datasetUri);
    String query = storageUri.getQuery();
    Preconditions.checkNotNull(query, "URI is not a dataset URI: " + datasetUri);
    
    Map<String, String> queryParams = QUERY_SPLITTER.split(query);
    return queryParams;
  }
  
  private static <E> PartitionKey getPartitionKey(Dataset<E> dataset, String keyString) {
    AbstractDataset<E> abstractDataset = (AbstractDataset<E>) dataset;
    return abstractDataset.toPartitionKey(keyString);
  }
  
  static <E> String getUri(Dataset<E> dataset, @Nullable PartitionKey partitionKey) {
    String repositoryUriString = dataset.getDescriptor().getProperty(
        AbstractDatasetRepository.REPOSITORY_URI_PROPERTY_NAME);
    Preconditions.checkNotNull(repositoryUriString, "Dataset is not in a repository");
    
    URI storageUri = DatasetRepositories.getStorageUri(repositoryUriString);

    String datasetUri = String.format("%s%sdataset-name=%s",repositoryUriString,
        storageUri.getQuery() == null ? "?" : "&", dataset.getName());
    if (partitionKey != null) {
      return datasetUri + "&partition-key=" + partitionKey;
    }
    
    return datasetUri;
  }

  static PartitionKey getPartitionKey(String datasetUri) {
    Map<String, String> queryParams = getQueryParams(datasetUri);
    String keyString = queryParams.get(PARTITION_KEY_PARAM);
    if (keyString == null) {
      return null;
    }
    return getPartitionKey(load(datasetUri, LoadFlag.BASE_DATASET_ONLY), keyString);
  }
  
}
