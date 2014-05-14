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
package org.kitesdk.data.crunch;

import com.google.common.base.Preconditions;
import org.apache.crunch.SourceTarget;
import org.apache.crunch.Target;
import org.apache.crunch.io.CrunchOutputs;
import org.apache.crunch.io.FormatBundle;
import org.apache.crunch.io.MapReduceTarget;
import org.apache.crunch.io.OutputHandler;
import org.apache.crunch.types.Converter;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.AvroType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.kitesdk.data.mapreduce.DatasetKeyOutputFormat;

class DatasetTarget<E> implements MapReduceTarget {

  FormatBundle formatBundle;

  public DatasetTarget(String datasetUri, boolean freeze) {
    this.formatBundle = FormatBundle.forOutput(DatasetKeyOutputFormat.class);
    formatBundle.set(DatasetKeyOutputFormat.KITE_DATASET_URI, datasetUri);
    formatBundle.set(DatasetKeyOutputFormat.KITE_FREEZE_DATASET, Boolean.toString(freeze));
  }

  @Override
  public Target outputConf(String key, String value) {
    formatBundle.set(key, value);
    return this;
  }

  @Override
  public boolean handleExisting(WriteMode writeMode, long l, Configuration entries) {
    // currently don't check for existing outputs
    return false;
  }

  @Override
  public boolean accept(OutputHandler handler, PType<?> ptype) {
    if (!(ptype instanceof AvroType)) {
      return false;
    }
    handler.configure(this, ptype);
    return true;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Converter<?, ?, ?, ?> getConverter(PType<?> ptype) {
    return new KeyConverter<E>((AvroType<E>) ptype);
  }

  @Override
  public <T> SourceTarget<T> asSourceTarget(PType<T> tpType) {
    return null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void configureForMapReduce(Job job, PType<?> ptype, Path outputPath, String name) {

    Preconditions.checkNotNull(name, "Output name should not be null"); // see CRUNCH-82

    Converter converter = getConverter(ptype);
    Class<?> keyClass = converter.getKeyClass();
    Class<?> valueClass = Void.class;

    CrunchOutputs.addNamedOutput(job, name, formatBundle, keyClass, valueClass);
    job.setOutputFormatClass(formatBundle.getFormatClass());
    formatBundle.configure(job.getConfiguration());
  }
}
