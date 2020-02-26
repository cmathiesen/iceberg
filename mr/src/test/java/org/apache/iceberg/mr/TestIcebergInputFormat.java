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

import com.google.common.collect.Lists;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.StandaloneHiveRunner;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.iceberg.Files;
import org.apache.iceberg.GenericManifestFile;
import org.apache.iceberg.GenericPartitionFieldSummary;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.List;

@RunWith(StandaloneHiveRunner.class)
public class TestIcebergInputFormat<T> {

  @HiveSQL(files = {}, autoStart = true)
  private HiveShell shell;

  @Test
  public void testGetSplits() {
    shell.execute("CREATE DATABASE source_db");
    shell.execute(new StringBuilder()
        .append("CREATE TABLE source_db.table_a ")
        //.append("(name STRING, salary INT) ")
        .append("ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' ")
        .append("STORED AS ")
        .append("INPUTFORMAT 'org.apache.iceberg.mr.IcebergInputFormat' ")
        .append("OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' ")
        .append("LOCATION '/Users/cmathiesen/projects/opensource/forks/eg-iceberg-fork/incubator-iceberg/mr/src/test/resources/test-table' ")
        .append("TBLPROPERTIES ('avro.schema.literal' = '{\"namespace\":\"source_db\",\"type\":\"record\",\"name\":\"table_a\",\"fields\":[{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null,\"field_id\":1},{\"name\":\"salary\",\"type\":[\"null\",\"long\"],\"default\":null,\"field_id\":2}]}')")
        .toString());

    List<Object[]> result = shell.executeStatement("SELECT * FROM source_db.table_a WHERE salary=3000");
  }

  @Test
  public void testReadingManifestListFile() {
    InputFile file = Files.localInput("/Users/cmathiesen/projects/opensource/forks/eg-iceberg-fork/incubator-iceberg/mr/src/test/resources/test-table/metadata/snap-7829799286772121706-1-1a3ffe32-d8da-47cf-9a8c-0e4c889a3a4c.avro");
    //Code from BaseSnapshot on trying to read a manifest-list file
    CloseableIterable<ManifestFile> manifests = Avro
        .read(file)
        .rename("manifest_file", GenericManifestFile.class.getName())
        .rename("partitions", GenericPartitionFieldSummary.class.getName())
        // 508 is the id used for the partition field, and r508 is the record name created for it in Avro schemas
        .rename("r508", GenericPartitionFieldSummary.class.getName())
        .project(ManifestFile.schema())
        .reuseContainers(false)
        .build();
    List<ManifestFile> files = Lists.newArrayList(manifests);
  }

  @Test
  public void testInputSplits() throws IOException {
    Configuration conf = new Configuration(false);
    conf.set("location", "file:/Users/cmathiesen/projects/opensource/forks/eg-iceberg-fork/incubator-iceberg/mr/src/test/resources/test-table");
    JobConf jobConf = new JobConf(conf);

    InputFormat inputFormat = ReflectionUtils.newInstance(IcebergInputFormat.class, conf);
    InputSplit[] splits = inputFormat.getSplits(jobConf, 3);
    RecordReader reader = inputFormat.getRecordReader(splits[0], jobConf, null);
    while (reader.next(reader.createKey(), reader.createValue())) {
      System.out.println(reader.createValue());
    }
  }

  @Test
  public void testReadingParquet() {
    InputFile file = Files.localInput("/Users/cmathiesen/projects/opensource/forks/eg-iceberg-fork/incubator-iceberg/mr/src/test/resources/test-table/data/00000-1-c7557bc3-ae0d-46fb-804e-e9806abf81c7-00000.parquet");
    Parquet.ReadBuilder builder = Parquet.read(file);
    CloseableIterable<T> reader = builder.build();
    System.out.println("test");
  }
}
