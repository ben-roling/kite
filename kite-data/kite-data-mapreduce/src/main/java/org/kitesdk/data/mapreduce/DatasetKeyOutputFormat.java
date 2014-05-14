/**
 * Copyright 2014 Cloudera Inc.
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
package org.kitesdk.data.mapreduce;

import com.google.common.annotations.Beta;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.kitesdk.compat.Hadoop;
import org.kitesdk.data.Dataset;
import org.kitesdk.data.DatasetDescriptor;
import org.kitesdk.data.DatasetRepositories;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.PartitionKey;
import org.kitesdk.data.Datasets.LoadFlag;
import org.kitesdk.data.spi.AbstractDataset;
import org.kitesdk.data.spi.AbstractDatasetRepository;
import org.kitesdk.data.spi.FieldPartitioner;
import org.kitesdk.data.spi.Mergeable;
import org.kitesdk.data.spi.partition.IdentityFieldPartitioner;

/**
 * A MapReduce {@code OutputFormat} for writing to a {@link Dataset}.
 *
 * Since a {@code Dataset} only contains entities (not key/value pairs), this output
 * format ignores the value.
 *
 * @param <E> The type of entities in the {@code Dataset}.
 */
@Beta
public class DatasetKeyOutputFormat<E> extends OutputFormat<E, Void> {

  public static final String KITE_DATASET_URI = "kite.outputDatasetUri";
  public static final String KITE_FREEZE_DATASET = "kite.freezeDataset";

  public static void setDatasetUri(Job job, String uri) {
    job.getConfiguration().set(KITE_DATASET_URI, uri);
  }

  static class DatasetRecordWriter<E> extends RecordWriter<E, Void> {

    private final DatasetWriter<E> datasetWriter;
    private final Map<String, Object> partitionFieldValues;

    public DatasetRecordWriter(Dataset<E> dataset, Map<String, Object> partitionFieldValues) {
      this.datasetWriter = dataset.newWriter();
      this.datasetWriter.open();
      this.partitionFieldValues = partitionFieldValues;
    }

    @Override
    public void write(E key, Void v) {
      /**
       * TODO this feature of auto-populating partition fields could use at least a couple of enhancements:
       * 1) It could be made optional as some consumers may not like it
       * 2) It could be enhanced to use reflection when using Avro reflection-based entities
       */
      if (key instanceof IndexedRecord) {
        IndexedRecord record = (IndexedRecord) key;
        for (Entry<String, Object> partitionFieldValue : partitionFieldValues.entrySet()) {
          int fieldPos = record.getSchema().getField(partitionFieldValue.getKey()).pos();
          record.put(fieldPos, partitionFieldValue.getValue());
        }
      }
      datasetWriter.write(key);
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) {
      datasetWriter.close();
    }
  }

  static class NullOutputCommitter extends OutputCommitter {
    @Override
    public void setupJob(JobContext jobContext) { }

    @Override
    public void setupTask(TaskAttemptContext taskContext) { }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext) {
      return false;
    }

    @Override
    public void commitTask(TaskAttemptContext taskContext) { }

    @Override
    public void abortTask(TaskAttemptContext taskContext) { }
  }

  static class MergeOutputCommitter<E> extends OutputCommitter {
    @Override
    public void setupJob(JobContext jobContext) {
      createJobDataset(jobContext);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void commitJob(JobContext jobContext) throws IOException {
      Configuration conf = Hadoop.JobContext.getConfiguration.invoke(jobContext);
      Dataset<E> dataset = loadTopLevelDataset(conf);
      Dataset<E> jobDataset = loadJobDataset(jobContext);
      
      ((Mergeable<Dataset<E>>) dataset).merge(jobDataset);
      
      if (conf.getBoolean(KITE_FREEZE_DATASET, false)) {
        // re-load the dataset in case the URI points to a partition
        dataset = loadDataset(conf);
        dataset.freeze();
      }
      deleteJobDataset(jobContext);
    }

    @Override
    public void abortJob(JobContext jobContext, JobStatus.State state) {
      deleteJobDataset(jobContext);
    }

    @Override
    public void setupTask(TaskAttemptContext taskContext) {
      // do nothing: the task attempt dataset is created in getRecordWriter
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext) {
      return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void commitTask(TaskAttemptContext taskContext) throws IOException {
      Dataset<E> taskAttemptDataset = loadTaskAttemptDataset(taskContext);
      if (taskAttemptDataset != null) {
        Dataset<E> jobDataset = loadJobDataset(taskContext);
        ((Mergeable<Dataset<E>>) jobDataset).merge(taskAttemptDataset);
        deleteTaskAttemptDataset(taskContext);
      }
    }

    @Override
    public void abortTask(TaskAttemptContext taskContext) {
      deleteTaskAttemptDataset(taskContext);
    }
  }

  @Override
  public RecordWriter<E, Void> getRecordWriter(TaskAttemptContext taskAttemptContext) {
    Configuration conf = Hadoop.TaskAttemptContext
        .getConfiguration.invoke(taskAttemptContext);
    Dataset<E> dataset = loadDataset(conf);
    
    // grab identity field values from the PartitionKey for partitioned datasets
    Map<String, Object> partitionFieldValues = new HashMap<String, Object>();
    PartitionKey partitionKey = ((AbstractDataset) dataset).getPartitionKey();
    if (partitionKey != null) {
      DatasetDescriptor topLevelDescriptor = loadTopLevelDataset(conf).getDescriptor();
      List<FieldPartitioner> fieldPartitioners = topLevelDescriptor.getPartitionStrategy().getFieldPartitioners();
      for (int i = 0; i < fieldPartitioners.size(); i++) {
        FieldPartitioner fieldPartitioner = fieldPartitioners.get(i);
        if (fieldPartitioner instanceof IdentityFieldPartitioner<?>) {
          IdentityFieldPartitioner<?> identityFieldPartitioner = (IdentityFieldPartitioner<?>) fieldPartitioner;
          partitionFieldValues.put(identityFieldPartitioner.getSourceName(), partitionKey.get(i));
        }
      }
    }

    if (usePerTaskAttemptDatasets(dataset)) {
      dataset = loadOrCreateTaskAttemptDataset(taskAttemptContext, partitionKey);
    }

    return new DatasetRecordWriter<E>(dataset, partitionFieldValues);
  }
  
  @Override
  public void checkOutputSpecs(JobContext jobContext) {
    // always run
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) {
    Configuration conf = Hadoop.TaskAttemptContext
        .getConfiguration.invoke(taskAttemptContext);
    Dataset<E> dataset = loadTopLevelDataset(conf);
    return usePerTaskAttemptDatasets(dataset) ?
        new MergeOutputCommitter<E>() : new NullOutputCommitter();
  }

  private static <E> boolean usePerTaskAttemptDatasets(Dataset<E> dataset) {
    // new API output committers are not called properly in Hadoop 1
    return !isHadoop1() && dataset instanceof Mergeable;
  }

  private static boolean isHadoop1() {
    return !JobContext.class.isInterface();
  }

  private static DatasetRepository getDatasetRepository(JobContext jobContext) {
    Configuration conf = Hadoop.JobContext.getConfiguration.invoke(jobContext);
    return DatasetRepositories.open(conf.get(KITE_DATASET_URI));
  }

  private static String getJobDatasetName(JobContext jobContext) {
    Configuration conf = Hadoop.JobContext.getConfiguration.invoke(jobContext);
    return loadDataset(conf).getName() + "_" + jobContext.getJobID().toString();
  }

  private static String getTaskAttemptDatasetName(TaskAttemptContext taskContext) {
    Configuration conf = Hadoop.TaskAttemptContext
        .getConfiguration.invoke(taskContext);
    return loadDataset(conf).getName() + "_" + taskContext.getTaskAttemptID().toString();
  }

  private static <E> Dataset<E> loadDataset(Configuration conf) {
    return Datasets.load(conf.get(KITE_DATASET_URI), LoadFlag.AUTO_CREATE_PARTITION);
  }
  
  private static <E> Dataset<E> loadTopLevelDataset(Configuration conf) {
    Dataset<E> dataset = loadDataset(conf);
    
    // TODO: replace with View#getDataset to get the top-level dataset
    DatasetRepository repo = DatasetRepositories.open(getRepositoryUri(dataset));
    Dataset<E> topLevelDataset = repo.load(dataset.getName());
    return topLevelDataset;
  }
  
  private static <E> String getRepositoryUri(Dataset<E> dataset) {
    return dataset.getDescriptor().getProperty(
        AbstractDatasetRepository.REPOSITORY_URI_PROPERTY_NAME);
  }

  private static <E> Dataset<E> createJobDataset(JobContext jobContext) {
    Configuration conf = Hadoop.JobContext.getConfiguration.invoke(jobContext);
    Dataset<Object> dataset = loadTopLevelDataset(conf);
    String jobDatasetName = getJobDatasetName(jobContext);
    DatasetRepository repo = getDatasetRepository(jobContext);
    return repo.create(jobDatasetName, copy(dataset.getDescriptor()));
  }

  private static <E> Dataset<E> loadJobDataset(JobContext jobContext) {
    DatasetRepository repo = getDatasetRepository(jobContext);
    return repo.load(getJobDatasetName(jobContext));
  }

  private static void deleteJobDataset(JobContext jobContext) {
    DatasetRepository repo = getDatasetRepository(jobContext);
    repo.delete(getJobDatasetName(jobContext));
  }

  private static <E> Dataset<E> loadOrCreateTaskAttemptDataset(TaskAttemptContext taskAttemptContext, PartitionKey partitionKey) {
    Configuration conf = Hadoop.TaskAttemptContext
        .getConfiguration.invoke(taskAttemptContext);
    Dataset<E> topLevelDataset = loadTopLevelDataset(conf);
    String taskAttemptDatasetName = getTaskAttemptDatasetName(taskAttemptContext);
    DatasetRepository repo = getDatasetRepository(taskAttemptContext);
    Dataset<E> dataset;
    if (repo.exists(taskAttemptDatasetName)) {
      dataset = repo.load(taskAttemptDatasetName);
    } else {
      dataset = repo.create(taskAttemptDatasetName, copy(topLevelDataset.getDescriptor()));
    }
    if (partitionKey != null) {
      dataset = dataset.getPartition(partitionKey, true);
    }
    return dataset;
  }

  private static <E> Dataset<E> loadTaskAttemptDataset(TaskAttemptContext taskContext) {
    DatasetRepository repo = getDatasetRepository(taskContext);
    String taskAttemptDatasetName = getTaskAttemptDatasetName(taskContext);
    if (repo.exists(taskAttemptDatasetName)) {
      return repo.load(taskAttemptDatasetName);
    }
    return null;
  }

  private static void deleteTaskAttemptDataset(TaskAttemptContext taskContext) {
    DatasetRepository repo = getDatasetRepository(taskContext);
    String taskAttemptDatasetName = getTaskAttemptDatasetName(taskContext);
    if (repo.exists(taskAttemptDatasetName)) {
      repo.delete(taskAttemptDatasetName);
    }
  }

  private static DatasetDescriptor copy(DatasetDescriptor descriptor) {
    // location must be null when creating a new dataset
    return new DatasetDescriptor.Builder(descriptor).location((URI) null).build();
  }

}
