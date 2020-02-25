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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.serde2.avro.AvroGenericRecordWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.GenericManifestFile;
import org.apache.iceberg.GenericPartitionFieldSummary;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.shaded.org.apache.avro.generic.GenericData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.apache.iceberg.Files.localInput;

public class IcebergInputFormat implements InputFormat {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergInputFormat.class);

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    String metadataPath = getMetadataPath(job);
    List<DataFile> dataFiles = getDataFiles(getManifestListPath(metadataPath));
    return createSplits(dataFiles);
  }

  private InputSplit[] createSplits(List<DataFile> dataFiles) {
    InputSplit[] splits = new InputSplit[dataFiles.size()];
    for (int i = 0; i < dataFiles.size(); i++) {
      splits[i] = new IcebergSplit(dataFiles.get(i));
    }
    return splits;
  }

  @Override
  public RecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
    return new IcebergRecordReader(split, job);
  }

  public class IcebergRecordReader implements RecordReader<Void, AvroGenericRecordWritable> {
    private JobConf context;
    private IcebergSplit split;
    private Iterator<GenericData.Record> recordIterator;
    private Schema icebergSchema;
    private org.apache.avro.Schema avroSchema;

    public IcebergRecordReader(InputSplit split, JobConf conf) throws IOException {
      this.split = (IcebergSplit) split;
      this.context = conf;
      initialise();
    }

    private void initialise() throws IOException {
      //TODO Build different readers depending on file type
      icebergSchema = getSchema(getMetadataPath(context));
      avroSchema = getAvroSchema(AvroSchemaUtil.convert(icebergSchema, "temp").toString().replaceAll("-", "_"));

      CloseableIterable<GenericData.Record> reader = buildParquetReader(Files.localInput(split.getFile().path().toString()), icebergSchema, false);
      recordIterator = reader.iterator();
    }

    @Override
    public boolean next(Void key, AvroGenericRecordWritable value) throws IOException {
      if (recordIterator.hasNext()) {
        GenericData.Record shadedRecord = recordIterator.next();
        org.apache.avro.generic.GenericData.Record record = getUnshadedRecord(avroSchema, shadedRecord);
        value.setRecord(record);
        return true;
      }
      return false;
    }

    @Override
    public Void createKey() {
      return null;
    }

    @Override
    public AvroGenericRecordWritable createValue() {
      AvroGenericRecordWritable record = new AvroGenericRecordWritable();
      record.setFileSchema(avroSchema); //Would the schema ever change between records? Maybe, so this might need to be set in the next() method too
      return record;
    }

    @Override
    public long getPos() throws IOException {
      return 0;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public float getProgress() throws IOException {
      return 0;
    }
  }

  private static class IcebergSplit implements Writable, InputSplit {

    private DataFile file;

    IcebergSplit(DataFile file) {
      this.file = file;
    }

    @Override
    public long getLength() throws IOException {
      return 0;
    }

    @Override
    public String[] getLocations() throws IOException {
      return new String[0];
    }

    @Override
    public void write(DataOutput out) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {

    }

    public DataFile getFile() {
      return file;
    }
  }

  // FIXME: use generic reader function
  private static CloseableIterable buildParquetReader(InputFile file, Schema schema, boolean reuseContainers) {
    Parquet.ReadBuilder builder = Parquet.read(file).project(schema);

    if (reuseContainers) {
      builder.reuseContainers();
    }

    return builder.build();
  }

  private Schema getSchema(String pathToMetaData) throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode node = objectMapper.readTree(new File(pathToMetaData));
    JsonNode schemaField = node.get("schema");
    return SchemaParser.fromJson(schemaField.toString());
  }

  private org.apache.avro.Schema getAvroSchema(String stringSchema) {
    org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
    return parser.parse(stringSchema);
  }

  //TODO: add error handling in case file doesn't contain what we expect
  private String getVersionHint(String location) throws IOException {
    FileReader fileReader = new FileReader(location);
    BufferedReader bufferedReader = new BufferedReader(fileReader);
    return bufferedReader.readLine();
  }

  private String getMetadataPath(JobConf conf) throws IOException {
    //TODO: make this more generic for other file systems, like HDFS
    String tableDir = StringUtils.substringAfter(conf.get("location"), "file:");
    String version = getVersionHint(tableDir + "/metadata/version-hint.text"); //Checks for latest metadata version
    return tableDir + "/metadata/" + getMetadataFilename(version);
  }

  private String getMetadataFilename(String version) {
    return "v" + version + ".metadata.json";
  }

  private String getManifestListPath(String pathToJSON) throws IOException {
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode node = objectMapper.readTree(new File(pathToJSON));
    JsonNode snapshotNode = node.get("snapshots").get(0);
    String manifest = snapshotNode.get("manifest-list").asText();
    return manifest;
  }

  //Had to change ManifestReader.read to public to get this - a way of getting around this?
  private List<DataFile> getDataFiles(String path) throws IOException {
    List<DataFile> dataFiles = Lists.newArrayList();
    try (CloseableIterable<ManifestFile> files = Avro.read(localInput(path))
        .rename("manifest_file", GenericManifestFile.class.getName())
        .rename("partitions", GenericPartitionFieldSummary.class.getName())
        .rename("r508", GenericPartitionFieldSummary.class.getName())
        .project(ManifestFile.schema())
        .reuseContainers(false)
        .build()) {

      List<ManifestFile> manifests = Lists.newLinkedList(files);
      for (ManifestFile file : manifests) {
        ManifestReader reader = ManifestReader.read(localInput(file.path()));
        List<DataFile> inputFiles = Lists.newArrayList(reader.iterator());
        dataFiles.addAll(inputFiles);
      }
    }
    return dataFiles;
  }

  private org.apache.avro.generic.GenericData.Record getUnshadedRecord(org.apache.avro.Schema schema, GenericData.Record shadedRecord) {
    List<org.apache.iceberg.shaded.org.apache.avro.Schema.Field> fields = shadedRecord.getSchema().getFields();
    GenericRecordBuilder builder = new GenericRecordBuilder(schema);
    for (org.apache.iceberg.shaded.org.apache.avro.Schema.Field field : fields) {
      Object value = shadedRecord.get(field.name());
      builder.set(field.name(), value);
    }
    return builder.build();
  }
}
