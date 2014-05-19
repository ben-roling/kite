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
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetRepositories;
import org.kitesdk.data.DatasetRepository;
import org.kitesdk.data.DatasetWriter;
import org.kitesdk.data.Datasets;
import org.kitesdk.data.PartitionKey;
import org.kitesdk.data.View;
import org.kitesdk.data.Datasets.LoadFlag;
import org.kitesdk.data.impl.Accessor;
import org.kitesdk.data.spi.AbstractDataset;
import org.kitesdk.data.spi.AbstractRefinableView;
import org.kitesdk.data.spi.Constraints;
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
  public static final String KITE_CONSTRAINTS = "kite.outputConstraints";

  public static void setDatasetUri(Job job, String uri) {
    job.getConfiguration().set(KITE_DATASET_URI, uri);
  }

  public static <E> void setView(Job job, View<E> view) {
    setView(job.getConfiguration(), view);
  }

  public static <E> void setView(Configuration conf, View<E> view) {
    if (view instanceof AbstractRefinableView) {
      conf.set(KITE_CONSTRAINTS,
          Constraints.serialize(((AbstractRefinableView) view).getConstraints()));
    }
  }

  static class DatasetRecordWriter<E> extends RecordWriter<E, Void> {

    private final DatasetWriter<E> datasetWriter;
    private final Map<String, Object> partitionFieldValues;

    public DatasetRecordWriter(View<E> view, Map<String, Object> partitionFieldValues) {
      this.datasetWriter = view.newWriter();
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
      Dataset<E> topLevelDataset = loadTopLevelDataset(conf);
      Dataset<E> jobDataset = loadJobDataset(jobContext);
      
      PartitionKey partitionKey = getPartitionKey(conf);
      if (partitionKey != null) {
        Dataset<E> jobPartition = jobDataset.getPartition(partitionKey, false);
        if (conf.getBoolean(KITE_FREEZE_DATASET, false)) {
          jobPartition.freeze();
        }
        ((Mergeable<Dataset<E>>) topLevelDataset).merge(jobPartition);
      }
      else {
        ((Mergeable<Dataset<E>>) topLevelDataset).merge(jobDataset);
        if (conf.getBoolean(KITE_FREEZE_DATASET, false)) {
          topLevelDataset.freeze();
        }
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
  @SuppressWarnings("unchecked")
  public RecordWriter<E, Void> getRecordWriter(TaskAttemptContext taskAttemptContext) {
    Configuration conf = Hadoop.TaskAttemptContext
        .getConfiguration.invoke(taskAttemptContext);
    
    // grab identity field values from the PartitionKey for partitioned datasets
    Map<String, Object> partitionFieldValues = new HashMap<String, Object>();
    PartitionKey partitionKey = getPartitionKey(conf);
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

    Dataset<E> topLevelDataset = loadTopLevelDataset(conf);
    Dataset<E> dataset;
    if (usePerTaskAttemptDatasets(topLevelDataset)) {
      dataset = loadOrCreateTaskAttemptDataset(taskAttemptContext, partitionKey);
    }
    else {
      dataset = loadDataset(conf);
    }
	
  	String constraintsString = conf.get(KITE_CONSTRAINTS);
  	if (constraintsString != null) {
      Constraints constraints = Constraints.deserialize(constraintsString);
  	  if (dataset instanceof AbstractDataset) {
        return new DatasetRecordWriter<E>(((AbstractDataset) dataset).filter(constraints), partitionFieldValues);
      }
      throw new DatasetException("Cannot find view from constraints for " + dataset);
    } else {
      return new DatasetRecordWriter<E>(dataset, partitionFieldValues);
    }
  }

  private static PartitionKey getPartitionKey(Configuration conf) {
    String datasetUri = conf.get(KITE_DATASET_URI);
    return Accessor.getDefault().getPartitionKey(datasetUri);
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
    return loadTopLevelDataset(conf).getName() + "_" + jobContext.getJobID().toString();
  }

  private static String getTaskAttemptDatasetName(TaskAttemptContext taskContext) {
    Configuration conf = Hadoop.TaskAttemptContext
        .getConfiguration.invoke(taskContext);
    return loadTopLevelDataset(conf).getName() + "_" + taskContext.getTaskAttemptID().toString();
  }

  private static <E> Dataset<E> loadDataset(Configuration conf) {
    return Datasets.load(conf.get(KITE_DATASET_URI), LoadFlag.AUTO_CREATE_PARTITION);
  }
  
  private static <E> Dataset<E> loadTopLevelDataset(Configuration conf) {
    return Datasets.load(conf.get(KITE_DATASET_URI), LoadFlag.BASE_DATASET_ONLY);
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
