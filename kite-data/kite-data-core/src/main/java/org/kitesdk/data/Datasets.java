/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.data;

import com.google.common.base.Preconditions;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.kitesdk.data.spi.Registration;

public class Datasets {

  private static final String DATASET_SCHEME = "dataset";
  private static final String VIEW_SCHEME = "view";

  /**
   * Load a {@link Dataset} for the given {@link URI}.
   * <p>
   * If the URI is a view URI, the underlying Dataset will be returned.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uri a {@code Dataset} or {@code View} URI.
   * @param <E> The type of entities stored in the {@code Dataset}.
   * @param <D> The type of {@code Dataset} expected.
   * @return a {@code Dataset} for the given URI.
   */
  public static <E, D extends Dataset<E>> D load(URI uri) {
    Preconditions.checkArgument(
        VIEW_SCHEME.equals(uri.getScheme()) ||
        DATASET_SCHEME.equals(uri.getScheme()),
        "Not a dataset or view URI: " + uri);
    return Registration.<E, D>load(URI.create(uri.getRawSchemeSpecificPart()));
  }

  /**
   * Load a {@link Dataset} for the given URI string.
   * <p>
   * If the URI is a view URI, the underlying Dataset will be returned.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uriString a {@code Dataset} or {@code View} URI.
   * @param <E> The type of entities stored in the {@code Dataset}.
   * @param <D> The type of {@code Dataset} expected.
   * @return a {@code Dataset} for the given URI.
   */
  public static <E, D extends Dataset<E>> D load(String uriString) {
    return Datasets.<E, D>load(URI.create(uriString));
  }

  /**
   * Load a {@link View} for the given {@link URI}.
   * <p>
   * If the URI is a dataset URI, the unfiltered Dataset will be returned.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uri a {@code Dataset} or {@code View} URI.
   * @param <E> The type of entities stored in the {@code Dataset}.
   * @param <V> The type of {@code View} expected.
   * @return a {@code View} for the given URI.
   */
  public static <E, V extends View<E>> V view(URI uri) {
    boolean isView = VIEW_SCHEME.equals(uri.getScheme());
    Preconditions.checkArgument(isView ||
        DATASET_SCHEME.equals(uri.getScheme()),
        "Not a dataset or view URI: " + uri);
    if (isView) {
      return Registration.<E, V>view(URI.create(uri.getRawSchemeSpecificPart()));
    } else {
      // if the URI isn't a view URI, only load the dataset
      return Registration.<E, V>load(URI.create(uri.getRawSchemeSpecificPart()));
    }
  }

  /**
   * Load a {@link View} for the given URI string.
   * <p>
   * If the URI is a dataset URI, the unfiltered Dataset will be returned.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param uriString a {@code Dataset} or {@code View} URI.
   * @param <E> The type of entities stored in the {@code Dataset}.
   * @param <V> The type of {@code View} expected.
   * @return a {@code View} for the given URI.
   */
  public static <E, V extends View<E>> V view(String uriString) {
    return Datasets.<E, V>view(URI.create(uriString));
  }

  /**
   * Load a {@link DatasetRepository} for the given dataset or view URI.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param datasetUri a {@code Dataset} or {@code View} URI.
   * @param <R> The type of {@code DatasetRepository} expected.
   * @return a {@code DatasetRepository} responsible for the given URI.
   */
  @SuppressWarnings("unchecked")
  public static <R extends DatasetRepository> R repositoryFor(URI datasetUri) {
    Preconditions.checkArgument(
        DATASET_SCHEME.equals(datasetUri.getScheme()) ||
        VIEW_SCHEME.equals(datasetUri.getScheme()),
        "Not a dataset or view URI: " + datasetUri);
    return (R) Registration.repoForDataset(
        URI.create(datasetUri.getRawSchemeSpecificPart()));
  }

  /**
   * Load a {@link DatasetRepository} for the given dataset or view URI string.
   * <p>
   * URI formats are defined by {@code Dataset} implementations, but must begin
   * with "dataset:" or "view:".
   *
   * @param datasetUriString a {@code Dataset} or {@code View} URI.
   * @param <R> The type of {@code DatasetRepository} expected.
   * @return a {@code DatasetRepository} responsible for the given URI.
   */
  public static <R extends DatasetRepository>
  R repositoryFor(String datasetUriString) {
    return repositoryFor(URI.create(datasetUriString));
  }
  
  
  /**
   * Builds dataset and view URIs 
   */
  public static class URIBuilder {
    private Dataset<?> dataset;
    private Map<String, Object> equalityConstraints = new HashMap<String, Object>();

    public URIBuilder(String repoUri, String datasetName) {
      dataset = DatasetRepositories.open(repoUri).load(datasetName);
    }
    
    public URIBuilder(URI repoUri, String datasetName) {
      this(repoUri.toString(), datasetName);
    }
    
    /**
     * Adds a view constraint equivalent to {@link Dataset#with(String, Object)}
     * 
     * @param name the field name of the Entity
     * @param value the field value
     * @return this builder
     */
    public URIBuilder with(String name, Object value) {
      equalityConstraints.put(name, value);
      return this;
    }
    
    /**
     * Returns the URI encompassing the given constraints.
     * 
     * @return the URI
     */
    public URI build() {
      URI datasetUri = dataset.getUri();
      if (equalityConstraints.isEmpty()) {
        return datasetUri;
      }
      
      URI storageUri = URI.create(datasetUri.getRawSchemeSpecificPart());
      String query = storageUri.getQuery();
      StringBuilder queryBuilder = new StringBuilder(query == null ? "" : query);
      for (Map.Entry<String, Object> entry : equalityConstraints.entrySet()) {
        if (queryBuilder.length() > 0) {
          queryBuilder.append("&");
        }
        queryBuilder.append(entry.getKey() + "=" + entry.getValue());
      }
      try {
        return new URI("view:" + storageUri.getScheme(),
            storageUri.getUserInfo(), storageUri.getHost(), storageUri.getPort(),
            storageUri.getPath(), queryBuilder.toString(), storageUri.getFragment());
      } catch (URISyntaxException e) {
        // mimicking behavior of URI.create()
        throw new IllegalArgumentException(e);
      }
    }
  }
}
