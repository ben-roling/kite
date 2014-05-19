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
package org.kitesdk.data.spi.filesystem;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.mapreduce.InputFormat;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.DatasetRepositoryException;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.Datasets.LoadFlag;
import org.kitesdk.data.PartitionKey;
import org.kitesdk.data.PartitionStrategy;
import org.kitesdk.data.RefinableView;
import org.kitesdk.data.impl.Accessor;
import org.kitesdk.data.spi.AbstractDataset;
import org.kitesdk.data.spi.Constraints;
import org.kitesdk.data.spi.FieldPartitioner;
import org.kitesdk.data.spi.InputFormatAccessor;
import org.kitesdk.data.spi.LastModifiedAccessor;
import org.kitesdk.data.spi.Mergeable;
import org.kitesdk.data.spi.PartitionListener;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.spi.SizeAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URI;
import java.util.List;

public class FileSystemDataset<E> extends AbstractDataset<E> implements
    Mergeable<FileSystemDataset<E>>, InputFormatAccessor<E>, LastModifiedAccessor,
    SizeAccessor {

  private static final Logger LOG = LoggerFactory
    .getLogger(FileSystemDataset.class);

  private final FileSystem fileSystem;
  private final Path directory;
  private final String name;
  private final DatasetDescriptor descriptor;
  private PartitionKey partitionKey;

  private final PartitionStrategy partitionStrategy;
  private final PartitionListener partitionListener;

  private final FileSystemView<E> unbounded;

  // reusable path converter, has no relevant state
  private final PathConversion convert;
  
  private boolean frozen;
  
  private static final String FROZEN_INDICATOR = "_FROZEN";

  FileSystemDataset(FileSystem fileSystem, Path directory, String name,
                    DatasetDescriptor descriptor,
                    @Nullable PartitionListener partitionListener) {

    this.fileSystem = fileSystem;
    this.directory = directory;
    this.name = name;
    this.descriptor = descriptor;
    this.partitionStrategy =
        descriptor.isPartitioned() ? descriptor.getPartitionStrategy() : null;
    this.partitionListener = partitionListener;
    this.convert = new PathConversion();

    this.unbounded = new FileSystemView<E>(this);
    // remove this.partitionKey for 0.14.0
    this.partitionKey = null;
  }

  FileSystemDataset(FileSystem fileSystem, Path directory, String name,
    DatasetDescriptor descriptor, @Nullable PartitionKey partitionKey,
    @Nullable PartitionListener partitionListener, boolean frozen) {
    this(fileSystem, directory, name, descriptor, partitionListener);
    this.partitionKey = partitionKey;
    this.frozen = frozen;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public DatasetDescriptor getDescriptor() {
    return descriptor;
  }

  @Override
  public PartitionKey getPartitionKey() {
    return partitionKey;
  }

  FileSystem getFileSystem() {
    return fileSystem;
  }

  Path getDirectory() {
    return directory;
  }

  PartitionListener getPartitionListener() {
    return partitionListener;
  }

  public boolean deleteAll() {
    Preconditions.checkState(!frozen, "Dataset must not be frozen");
    
    // no constraints, so delete is always aligned to partition boundaries
    return unbounded.deleteAllUnsafe();
  }

  public PathIterator pathIterator() {
    return unbounded.pathIterator();
  }

  /**
   * Returns an iterator that provides all leaf-level directories in this view.
   *
   * @return leaf-directory iterator
   */
  public Iterator<Path> dirIterator() {
    return unbounded.dirIterator();
  }

  @Override
  protected RefinableView<E> asRefinableView() {
    return unbounded;
  }
  
  @Override
  public FileSystemView<E> filter(Constraints c) {
    return unbounded.filter(c);
  }

  @Override
  @Nullable
  @Deprecated
  public Dataset<E> getPartition(PartitionKey key, boolean allowCreate) {
    Preconditions.checkState(descriptor.isPartitioned(),
      "Attempt to get a partition on a non-partitioned dataset (name:%s)",
      name);

    LOG.debug("Loading partition for key {}, allowCreate:{}", new Object[] {
      key, allowCreate });

    Path partitionDirectory = getPartitionDirectory(key);

    try {
      if (!fileSystem.exists(partitionDirectory)) {
        if (allowCreate) {
          fileSystem.mkdirs(partitionDirectory);
          if (partitionListener != null) {
            partitionListener.partitionAdded(name,
                toRelativeDirectory(key).toString());
          }
        } else {
          return null;
        }
      }
    } catch (IOException e) {
      throw new DatasetException(
          "Unable to locate or create dataset partition directory "
              + partitionDirectory, e);
    }

    return getPartition(key, partitionDirectory);
  }
  
  private Path getPartitionDirectory(PartitionKey key) {
    return fileSystem.makeQualified(
        toDirectoryName(directory, key));
  }
  
  private Dataset<E> getPartition(PartitionKey key, Path partitionDirectory) {
    int partitionDepth = key.getLength();
    PartitionStrategy subpartitionStrategy = Accessor.getDefault()
        .getSubpartitionStrategy(partitionStrategy, partitionDepth);

    return new FileSystemDataset.Builder()
        .name(name)
        .fileSystem(fileSystem)
        .descriptor(new DatasetDescriptor.Builder(descriptor)
            .location(partitionDirectory)
            .partitionStrategy(subpartitionStrategy)
            .build())
        .partitionKey(key)
        .partitionListener(partitionListener)
        .frozen(frozen)
        .build();
  }
  
  @Override
  @Deprecated
  public void dropPartition(PartitionKey key) {
    Preconditions.checkState(descriptor.isPartitioned(),
      "Attempt to drop a partition on a non-partitioned dataset (name:%s)",
      name);
    Preconditions.checkArgument(key != null, "Partition key may not be null");
    Preconditions.checkState(!frozen, "Dataset must not be frozen");

    LOG.debug("Dropping partition with key:{} dataset:{}", key, name);

    Path partitionDirectory = toDirectoryName(directory, key);

    try {
      if (!fileSystem.delete(partitionDirectory, true)) {
        throw new DatasetException("Partition directory " + partitionDirectory
          + " for key " + key + " does not exist");
      }
    } catch (IOException e) {
      throw new DatasetException(
          "Unable to locate or drop dataset partition directory "
              + partitionDirectory, e);
    }
  }

  @Override
  @Deprecated
  public Iterable<Dataset<E>> getPartitions() {
    Preconditions.checkState(descriptor.isPartitioned(),
      "Attempt to get partitions on a non-partitioned dataset (name:%s)",
      name);

    List<Dataset<E>> partitions = Lists.newArrayList();

    FileStatus[] fileStatuses;

    try {
      fileStatuses = fileSystem.listStatus(directory,
        PathFilters.notHidden());
    } catch (IOException e) {
      throw new DatasetException("Unable to list partition directory for directory " + directory, e);
    }

    for (FileStatus stat : fileStatuses) {
      Path p = fileSystem.makeQualified(stat.getPath());
      PartitionKey key = keyFromDirectory(p);
      PartitionStrategy subPartitionStrategy = Accessor.getDefault()
          .getSubpartitionStrategy(partitionStrategy, 1);
      Builder builder = new FileSystemDataset.Builder()
          .name(name)
          .fileSystem(fileSystem)
          .descriptor(new DatasetDescriptor.Builder(descriptor)
              .location(p)
              .partitionStrategy(subPartitionStrategy)
              .build())
          .partitionKey(key)
          .partitionListener(partitionListener);

      partitions.add(builder.<E>build());
    }

    return partitions;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("name", name)
      .add("descriptor", descriptor).add("directory", directory)
      .add("dataDirectory", directory).add("partitionKey", partitionKey)
      .toString();
  }

  @Override
  public void merge(FileSystemDataset<E> update) {
    Preconditions.checkState(!frozen, "Dataset must not be frozen");
    
    DatasetDescriptor updateDescriptor = update.getDescriptor();

    if (!updateDescriptor.getFormat().equals(descriptor.getFormat())) {
      throw new DatasetRepositoryException("Cannot merge dataset format " +
          updateDescriptor.getFormat() + " with format " + descriptor.getFormat());
    }

    if (updateDescriptor.isPartitioned() && descriptor.isPartitioned()) {
      verifyMatchingPartitionStrategy(updateDescriptor);
    }
    
    if (updateDescriptor.isPartitioned() && !descriptor.isPartitioned()) {
      throw new DatasetRepositoryException("Cannot merge a partitioned dataset into an"
          + " an upartitioned dataset");
    }

    if (!updateDescriptor.getSchema().equals(descriptor.getSchema())) {
      throw new DatasetRepositoryException("Cannot merge dataset schema " +
          updateDescriptor.getFormat() + " with schema " + descriptor.getFormat());
    }
    
    if (descriptor.isPartitioned() && !updateDescriptor.isPartitioned()) {
      mergeSubpartition(update);
    }
    else {
      Set<String> addedPartitions = Sets.newHashSet();
      for (Path path : update.pathIterator()) {
        URI relativePath = update.getDirectory().toUri().relativize(path.toUri());
        Path newPath = new Path(directory, new Path(relativePath));
        Path newPartitionDirectory = newPath.getParent();
        try {
          if (!fileSystem.exists(newPartitionDirectory)) {
            fileSystem.mkdirs(newPartitionDirectory);
          }
          LOG.debug("Renaming {} to {}", path, newPath);
          boolean renameOk = fileSystem.rename(path, newPath);
          if (!renameOk) {
            throw new DatasetException("Dataset merge failed during rename of " + path +
                " to " + newPath);
          }
        } catch (IOException e) {
          throw new DatasetIOException("Dataset merge failed", e);
        }
        if (descriptor.isPartitioned() && partitionListener != null) {
          String partition = newPartitionDirectory.toString();
          if (!addedPartitions.contains(partition)) {
            partitionListener.partitionAdded(name, partition);
            addedPartitions.add(partition);
          }
        }
      }
    }
  }

  private void mergeSubpartition(FileSystemDataset<E> update) {
    FileSystemDataset<E> updateTopLevel = (FileSystemDataset<E>) Datasets.load(
        update.getUri(), LoadFlag.BASE_DATASET_ONLY);
    DatasetDescriptor updateTopLevelDescriptor = updateTopLevel.getDescriptor();
    if (!updateTopLevelDescriptor.isPartitioned()) {
      throw new DatasetRepositoryException("Cannot merge unpartitioned dataset"
          + " into partitioned dataset unless it is a subpartition");
    }
    verifyMatchingPartitionStrategy(updateTopLevelDescriptor);
    
    URI relativePath = updateTopLevel.getDirectory().toUri().relativize(update.getDirectory().toUri());
    Path newPath = new Path(directory, new Path(relativePath));
    try {
      fileSystem.rename(update.getDirectory(), newPath);
    } catch (IOException e) {
      throw new DatasetException("Dataset merge failed during rename of " + directory +
          " to " + newPath);
    }
  }
  
  private void verifyMatchingPartitionStrategy(DatasetDescriptor updateDescriptor) {
    if (!updateDescriptor.getPartitionStrategy().equals(descriptor.getPartitionStrategy())) {
      throw new DatasetRepositoryException("Cannot merge dataset partition strategy " +
          updateDescriptor.getPartitionStrategy() + " with " + descriptor.getPartitionStrategy());
    }
  }

  @Override
  public InputFormat<E, Void> getInputFormat() {
    return new FileSystemViewKeyInputFormat<E>(this);
  }

  @SuppressWarnings("unchecked")
  private Path toDirectoryName(Path dir, PartitionKey key) {
    Path result = dir;
    for (int i = 0; i < key.getLength(); i++) {
      final FieldPartitioner fp = partitionStrategy.getFieldPartitioners().get(i);
      if (result != null) {
        result = new Path(result, convert.dirnameForValue(fp, key.get(i)));
      } else {
        result = new Path(convert.dirnameForValue(fp, key.get(i)));
      }
    }
    return result;
  }

  private Path toRelativeDirectory(PartitionKey key) {
    return toDirectoryName(null, key);
  }

  @SuppressWarnings("unchecked")
  public PartitionKey keyFromDirectory(Path dir) {
    final FieldPartitioner fp = partitionStrategy.getFieldPartitioners().get(0);
    final List<Object> values = Lists.newArrayList();

    if (partitionKey != null) {
      values.addAll(partitionKey.getValues());
    }

    values.add(convert.valueForDirname(fp, dir.getName()));

    return Accessor.getDefault().newPartitionKey(values.toArray());
  }

  @Override
  public long getSize() {
    long size = 0;
    for (Iterator<Path> i = dirIterator(); i.hasNext(); ) {
      Path dir = i.next();
      try {
        for (FileStatus st : fileSystem.listStatus(dir)) {
          size += st.getLen();
        }
      } catch (IOException e) {
        throw new DatasetIOException("Cannot find size of " + dir, e);
      }
    }
    return size;
  }

  @Override
  public long getLastModified() {
    long lastMod = -1;
    for (Iterator<Path> i = dirIterator(); i.hasNext(); ) {
      Path dir = i.next();
      try {
        for (FileStatus st : fileSystem.listStatus(dir)) {
          if (lastMod < st.getModificationTime()) {
            lastMod = st.getModificationTime();
          }
        }
      } catch (IOException e) {
        throw new DatasetIOException("Cannot find last modified time of of " + dir, e);
      }
    }
    return lastMod;
  }
  
  @Override
  public boolean isFrozen() {
    return frozen;
  }
  
  @Override
  public void freeze() {
    if (!isFrozen()) {
      try {
        fileSystem.createNewFile(new Path(getDirectory(), FROZEN_INDICATOR));
      } catch (IOException e) {
        throw new DatasetException("Could not create " + FROZEN_INDICATOR
            + " file in " + getDirectory(), e);
      }
      frozen = true;
    }
  }

  public static class Builder {

    private Configuration conf;
    private FileSystem fileSystem;
    private Path directory;
    private String name;
    private DatasetDescriptor descriptor;
    private PartitionKey partitionKey;
    private PartitionListener partitionListener;
    private boolean frozen;

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    protected Builder fileSystem(FileSystem fs) {
      this.fileSystem = fs;
      return this;
    }

    public Builder configuration(Configuration conf) {
      this.conf = conf;
      return this;
    }

     public Builder descriptor(DatasetDescriptor descriptor) {
      Preconditions.checkArgument(descriptor.getLocation() != null,
          "Dataset location cannot be null");

      this.descriptor = descriptor;

      return this;
    }

    Builder partitionKey(@Nullable PartitionKey partitionKey) {
      this.partitionKey = partitionKey;
      return this;
    }

    Builder partitionListener(@Nullable PartitionListener partitionListener) {
      this.partitionListener = partitionListener;
      return this;
    }
    
    Builder frozen(boolean frozen) {
      this.frozen = frozen;
      return this;
    }

    public <E> FileSystemDataset<E> build() {
      Preconditions.checkState(this.name != null, "No dataset name defined");
      Preconditions.checkState(this.descriptor != null,
        "No dataset descriptor defined");
      Preconditions.checkState((conf != null) || (fileSystem != null),
          "Configuration or FileSystem must be set");
      
      this.directory = new Path(descriptor.getLocation());

      if (fileSystem == null) {
        try {
          this.fileSystem = directory.getFileSystem(conf);
        } catch (IOException ex) {
          throw new DatasetException("Cannot access FileSystem", ex);
        }
      }
      
      Path absoluteDirectory = fileSystem.makeQualified(directory);
      try {
        if (fileSystem.exists(new Path(absoluteDirectory, FROZEN_INDICATOR))) {
          frozen = true;
        }
      } catch (IOException e) {
        throw new DatasetException("Cannot check for " + FROZEN_INDICATOR
            + " indicator in " + absoluteDirectory, e);
      }
      
      return new FileSystemDataset<E>(
          fileSystem, absoluteDirectory, name, descriptor, partitionKey,
          partitionListener, frozen);
    }
  }

  @Override
  public String getUri() {
    return Accessor.getDefault().getUri(this,
            partitionKey != null && !partitionKey.getValues().isEmpty() ? partitionKey
                .getValues().toString() : null);
  }
  
  @Override
  public PartitionKey toPartitionKey(String partitionKey) {
    Object[] keyValues = Iterables.toArray(Splitter.on(",").split(
        partitionKey.substring(1, partitionKey.length()-1)), Object.class);
    return descriptor.getPartitionStrategy().partitionKey(keyValues);
  }
}
