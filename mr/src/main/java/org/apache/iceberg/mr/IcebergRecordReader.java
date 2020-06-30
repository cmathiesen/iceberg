/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;

import java.util.List;
import java.util.Map;

public class IcebergRecordReader<T> {

  private boolean applyResidual;
  private boolean caseSensitive;
  private boolean reuseContainers;

  private void initialize(Configuration conf) {
    this.applyResidual = !conf.getBoolean(InputFormatConfig.SKIP_RESIDUAL_FILTERING, false);
    this.caseSensitive = conf.getBoolean(InputFormatConfig.CASE_SENSITIVE, true);
    this.reuseContainers = conf.getBoolean(InputFormatConfig.REUSE_CONTAINERS, false);
  }

  public CloseableIterable<T> createReader(Configuration config, FileScanTask currentTask, Schema readSchema) {
    initialize(config);
    DataFile file = currentTask.file();
    // TODO we should make use of FileIO to create inputFile
    InputFile inputFile = HadoopInputFile.fromLocation(file.path(), config);
    switch (file.format()) {
      case AVRO:
        return newAvroIterable(inputFile, currentTask, readSchema);
      case ORC:
        return newOrcIterable(inputFile, currentTask, readSchema);
      case PARQUET:
        return newParquetIterable(inputFile, currentTask, readSchema);
      case METADATA:
        return newMetadataIterable(currentTask.asDataTask(), readSchema);
      default:
        throw new UnsupportedOperationException(
                String.format("Cannot read %s file: %s", file.format().name(), file.path()));
    }
  }

  private CloseableIterable<T> newAvroIterable(InputFile inputFile, FileScanTask task, Schema readSchema) {
    Avro.ReadBuilder avroReadBuilder = Avro.read(inputFile).project(readSchema).split(task.start(), task.length());
    if (reuseContainers) {
      avroReadBuilder.reuseContainers();
    }
    avroReadBuilder.createReaderFunc(DataReader::create);
    return applyResidualFiltering(avroReadBuilder.build(), task.residual(), readSchema);
  }

  private CloseableIterable<T> newParquetIterable(InputFile inputFile, FileScanTask task, Schema readSchema) {
    Parquet.ReadBuilder parquetReadBuilder = Parquet
            .read(inputFile)
            .project(readSchema)
            .filter(task.residual())
            .caseSensitive(caseSensitive)
            .split(task.start(), task.length());
    if (reuseContainers) {
      parquetReadBuilder.reuseContainers();
    }

    parquetReadBuilder.createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(readSchema, fileSchema));

    return applyResidualFiltering(parquetReadBuilder.build(), task.residual(), readSchema);
  }

  private CloseableIterable<T> newOrcIterable(InputFile inputFile, FileScanTask task, Schema readSchema) {
    ORC.ReadBuilder orcReadBuilder = ORC
            .read(inputFile)
            .project(readSchema)
            .caseSensitive(caseSensitive)
            .split(task.start(), task.length());
    // ORC does not support reuse containers yet
    orcReadBuilder.createReaderFunc(fileSchema -> GenericOrcReader.buildReader(readSchema, fileSchema));
    return applyResidualFiltering(orcReadBuilder.build(), task.residual(), readSchema);
  }

  private CloseableIterable<T> newMetadataIterable(DataTask task, Schema readSchema) {
    CloseableIterable asStructLikeRows = task.rows();
    return CloseableIterable.transform(asStructLikeRows, row -> convertToRecord((StructLike) row, readSchema));
  }

  private CloseableIterable<T> applyResidualFiltering(CloseableIterable iter, Expression residual, Schema readSchema) {
    if (applyResidual && residual != null && residual != Expressions.alwaysTrue()) {
      Evaluator filter = new Evaluator(readSchema.asStruct(), residual, caseSensitive);
      return CloseableIterable.filter(iter, record -> filter.eval((StructLike) record));
    } else {
      return iter;
    }
  }

  private Record convertToRecord(StructLike structLike, Schema readSchema) {
    Record record = GenericRecord.create(readSchema);
    for(int i = 0; i < readSchema.columns().size(); i++) {
      Type type = readSchema.findType(readSchema.columns().get(i).name());
      record.set(i, fieldValue(type, structLike, i, readSchema));
    }
    return record;
  }

  private Class javaType(Schema readSchema, int column) {
    Type id = readSchema.findType(readSchema.columns().get(column).name());
    if (id.isMapType()) {
      return Map.class;
    } else if (id.isListType()) {
      return List.class;
    } else {
      return id.typeId().javaClass();
    }
  }

  private Object fieldValue(Type type, StructLike structLike, int column, Schema readSchema) {
    if (type instanceof Types.TimestampType) {
      Long value = (Long) structLike.get(column, javaType(readSchema, column));
      return ((Types.TimestampType) type).shouldAdjustToUTC() ? DateTimeUtil.timestamptzFromMicros(value) : DateTimeUtil.timestampFromMicros(value);
    } else {
      return structLike.get(column, javaType(readSchema, column));
    }
  }
}
