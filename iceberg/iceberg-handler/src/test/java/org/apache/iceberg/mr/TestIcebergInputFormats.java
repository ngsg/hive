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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.mr.hive.HiveIcebergInputFormat;
import org.apache.iceberg.mr.mapred.Container;
import org.apache.iceberg.mr.mapred.MapredIcebergInputFormat;
import org.apache.iceberg.mr.mapreduce.IcebergInputFormat;
import org.apache.iceberg.mr.mapreduce.IcebergSplit;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ThreadPools;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class TestIcebergInputFormats {

  public static final List<TestInputFormat.Factory<Record>> TESTED_INPUT_FORMATS = ImmutableList.of(
          TestInputFormat.newFactory("IcebergInputFormat", TestIcebergInputFormat::create),
          TestInputFormat.newFactory("MapredIcebergInputFormat", TestMapredIcebergInputFormat::create));

  private static final List<String> TESTED_FILE_FORMATS = ImmutableList.of("avro", "orc", "parquet");

  private static final Schema SCHEMA = new Schema(
          required(1, "data", Types.StringType.get()),
          required(2, "id", Types.LongType.get()),
          required(3, "date", Types.StringType.get()));

  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
          .identity("date")
          .bucket("id", 1)
          .build();

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  // before variables
  private Configuration conf;
  private TestHelper helper;
  private InputFormatConfig.ConfigBuilder builder;

  // parametrized variables
  private final TestInputFormat.Factory<Record> testInputFormat;
  private final FileFormat fileFormat;

  @Before
  public void before() throws IOException {
    conf = new JobConf();
    conf.set(CatalogUtil.ICEBERG_CATALOG_TYPE, Catalogs.LOCATION);
    HadoopTables tables = new HadoopTables(conf);

    File location = temp.newFolder(testInputFormat.name(), fileFormat.name());
    Assert.assertTrue(location.delete());

    helper = new TestHelper(conf, tables, location.toString(), SCHEMA, SPEC, fileFormat, temp);
    builder = new InputFormatConfig.ConfigBuilder(conf).readFrom(location.toString());
  }

  @Parameterized.Parameters(name = "testInputFormat = {0}, fileFormat = {1}")
  public static Object[][] parameters() {
    Object[][] parameters = new Object[TESTED_INPUT_FORMATS.size() * TESTED_FILE_FORMATS.size()][2];

    int idx = 0;

    for (TestInputFormat.Factory<Record> inputFormat : TESTED_INPUT_FORMATS) {
      for (String fileFormat : TESTED_FILE_FORMATS) {
        parameters[idx++] = new Object[] {inputFormat, fileFormat};
      }
    }

    return parameters;
  }

  public TestIcebergInputFormats(TestInputFormat.Factory<Record> testInputFormat, String fileFormat) {
    this.testInputFormat = testInputFormat;
    this.fileFormat = FileFormat.fromString(fileFormat);
  }

  @Test
  public void testUnpartitionedTable() throws Exception {
    helper.createUnpartitionedTable();
    List<Record> expectedRecords = helper.generateRandomRecords(1, 0L);
    helper.appendToTable(null, expectedRecords);

    testInputFormat.create(builder.conf()).validate(expectedRecords);
  }

  @Test
  public void testPartitionedTable() throws Exception {
    helper.createTable();
    List<Record> expectedRecords = helper.generateRandomRecords(1, 0L);
    expectedRecords.get(0).set(2, "2020-03-20");
    helper.appendToTable(Row.of("2020-03-20", 0), expectedRecords);

    testInputFormat.create(builder.conf()).validate(expectedRecords);
  }

  @Test
  public void testFilterExp() throws Exception {
    helper.createTable();

    List<Record> expectedRecords = helper.generateRandomRecords(2, 0L);
    expectedRecords.get(0).set(2, "2020-03-20");
    expectedRecords.get(1).set(2, "2020-03-20");

    DataFile dataFile1 = helper.writeFile(Row.of("2020-03-20", 0), expectedRecords);
    DataFile dataFile2 = helper.writeFile(Row.of("2020-03-21", 0), helper.generateRandomRecords(2, 0L));
    helper.appendToTable(dataFile1, dataFile2);

    builder.filter(Expressions.equal("date", "2020-03-20"));
    testInputFormat.create(builder.conf()).validate(expectedRecords);
  }

  @Test
  public void testResiduals() throws Exception {
    helper.createTable();

    List<Record> writeRecords = helper.generateRandomRecords(2, 0L);
    writeRecords.get(0).set(1, 123L);
    writeRecords.get(0).set(2, "2020-03-20");
    writeRecords.get(1).set(1, 456L);
    writeRecords.get(1).set(2, "2020-03-20");

    List<Record> expectedRecords = Lists.newArrayList();
    expectedRecords.add(writeRecords.get(0));

    DataFile dataFile1 = helper.writeFile(Row.of("2020-03-20", 0), writeRecords);
    DataFile dataFile2 = helper.writeFile(Row.of("2020-03-21", 0), helper.generateRandomRecords(2, 0L));
    helper.appendToTable(dataFile1, dataFile2);

    builder.filter(Expressions.and(
            Expressions.equal("date", "2020-03-20"),
            Expressions.equal("id", 123)));
    testInputFormat.create(builder.conf()).validate(expectedRecords);

    // skip residual filtering
    builder.skipResidualFiltering();
    testInputFormat.create(builder.conf()).validate(writeRecords);
  }

  @Test
  @Ignore
  // This test is ignored because for ARVO, the vectorized IcebergInputFormat.IcebergRecordReader doesn't support AVRO
  // and for ORC and PARQUET, IcebergInputFormat class ignores residuals
  // '... scan.filter(filter).ignoreResiduals()' and it is not compatible with this test
  public void testFailedResidualFiltering() throws Exception {
    Assume.assumeTrue("Vectorization is not yet supported for AVRO", this.fileFormat != FileFormat.AVRO);

    helper.createTable();

    List<Record> expectedRecords = helper.generateRandomRecords(2, 0L);
    expectedRecords.get(0).set(2, "2020-03-20");
    expectedRecords.get(1).set(2, "2020-03-20");

    helper.appendToTable(Row.of("2020-03-20", 0), expectedRecords);

    builder
        .useHiveRows()
        .filter(
            Expressions.and(Expressions.equal("date", "2020-03-20"), Expressions.equal("id", 0)));

    Assertions.assertThatThrownBy(() -> testInputFormat.create(builder.conf()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage(
            "Filter expression ref(name=\"id\") == 0 is not completely satisfied. Additional rows can be returned " +
                    "not satisfied by the filter expression");

    Assertions.assertThatThrownBy(() -> testInputFormat.create(builder.conf()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage(
            "Filter expression ref(name=\"id\") == 0 is not completely satisfied. Additional rows can be returned " +
                    "not satisfied by the filter expression");
  }

  @Test
  public void testProjection() throws Exception {
    helper.createTable();
    List<Record> inputRecords = helper.generateRandomRecords(1, 0L);
    helper.appendToTable(Row.of("2020-03-20", 0), inputRecords);

    Schema projection = TypeUtil.select(SCHEMA, ImmutableSet.of(1));
    builder.project(projection);

    List<Record> outputRecords = testInputFormat.create(builder.conf()).getRecords();

    Assert.assertEquals(inputRecords.size(), outputRecords.size());
    Assert.assertEquals(projection.asStruct(), outputRecords.get(0).struct());
  }

  private static final Schema LOG_SCHEMA = new Schema(
          Types.NestedField.optional(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "date", Types.StringType.get()),
          Types.NestedField.optional(3, "level", Types.StringType.get()),
          Types.NestedField.optional(4, "message", Types.StringType.get())
  );

  private static final PartitionSpec IDENTITY_PARTITION_SPEC =
          PartitionSpec.builderFor(LOG_SCHEMA).identity("date").identity("level").build();

  @Test
  public void testIdentityPartitionProjections() throws Exception {
    helper.createTable(LOG_SCHEMA, IDENTITY_PARTITION_SPEC);
    List<Record> inputRecords = helper.generateRandomRecords(10, 0L);

    Integer idx = 0;
    AppendFiles append = helper.table().newAppend();
    for (Record record : inputRecords) {
      record.set(1, "2020-03-2" + idx);
      record.set(2, idx.toString());
      append.appendFile(helper.writeFile(Row.of("2020-03-2" + idx, idx.toString()), ImmutableList.of(record)));
      idx += 1;
    }
    append.commit();

    // individual fields
    validateIdentityPartitionProjections(withColumns("date"), inputRecords);
    validateIdentityPartitionProjections(withColumns("level"), inputRecords);
    validateIdentityPartitionProjections(withColumns("message"), inputRecords);
    validateIdentityPartitionProjections(withColumns("id"), inputRecords);
    // field pairs
    validateIdentityPartitionProjections(withColumns("date", "message"), inputRecords);
    validateIdentityPartitionProjections(withColumns("level", "message"), inputRecords);
    validateIdentityPartitionProjections(withColumns("date", "level"), inputRecords);
    // out-of-order pairs
    validateIdentityPartitionProjections(withColumns("message", "date"), inputRecords);
    validateIdentityPartitionProjections(withColumns("message", "level"), inputRecords);
    validateIdentityPartitionProjections(withColumns("level", "date"), inputRecords);
    // full projection
    validateIdentityPartitionProjections(LOG_SCHEMA, inputRecords);
    // out-of-order triplets
    validateIdentityPartitionProjections(withColumns("date", "level", "message"), inputRecords);
    validateIdentityPartitionProjections(withColumns("level", "date", "message"), inputRecords);
    validateIdentityPartitionProjections(withColumns("date", "message", "level"), inputRecords);
    validateIdentityPartitionProjections(withColumns("level", "message", "date"), inputRecords);
    validateIdentityPartitionProjections(withColumns("message", "date", "level"), inputRecords);
    validateIdentityPartitionProjections(withColumns("message", "level", "date"), inputRecords);
  }

  private static Schema withColumns(String... names) {
    Map<String, Integer> indexByName = TypeUtil.indexByName(LOG_SCHEMA.asStruct());
    Set<Integer> projectedIds = Sets.newHashSet();
    for (String name : names) {
      projectedIds.add(indexByName.get(name));
    }
    return TypeUtil.select(LOG_SCHEMA, projectedIds);
  }

  private void validateIdentityPartitionProjections(Schema projectedSchema, List<Record> inputRecords) {
    builder.project(projectedSchema);
    List<Record> actualRecords = testInputFormat.create(builder.conf()).getRecords();

    Set<String> fieldNames = TypeUtil.indexByName(projectedSchema.asStruct()).keySet();

    for (int pos = 0; pos < inputRecords.size(); pos++) {
      Record inputRecord = inputRecords.get(pos);
      Record actualRecord = actualRecords.get(pos);
      Assert.assertEquals("Projected schema should match", projectedSchema.asStruct(), actualRecord.struct());

      for (String name : fieldNames) {
        Assert.assertEquals(
                "Projected field " + name + " should match", inputRecord.getField(name), actualRecord.getField(name));
      }
    }
  }

  @Test
  public void testSnapshotReads() throws Exception {
    helper.createUnpartitionedTable();

    List<Record> expectedRecords = helper.generateRandomRecords(1, 0L);
    helper.appendToTable(null, expectedRecords);
    long snapshotId = helper.table().currentSnapshot().snapshotId();

    helper.appendToTable(null, helper.generateRandomRecords(1, 0L));

    builder.snapshotId(snapshotId);
    testInputFormat.create(builder.conf()).validate(expectedRecords);
  }

  @Test
  public void testLocality() throws Exception {
    helper.createUnpartitionedTable();
    List<Record> expectedRecords = helper.generateRandomRecords(1, 0L);
    helper.appendToTable(null, expectedRecords);

    for (InputSplit split : testInputFormat.create(builder.conf()).getSplits()) {
      Assert.assertArrayEquals(IcebergSplit.ANYWHERE, split.getLocations());
    }

    builder.preferLocality();

    for (InputSplit split : testInputFormat.create(builder.conf()).getSplits()) {
      Assert.assertArrayEquals(new String[]{"localhost"}, split.getLocations());
    }
  }

  @Test
  public void testCustomCatalog() throws IOException {
    String warehouseLocation = temp.newFolder("hadoop_catalog").getAbsolutePath();
    conf.set("warehouse.location", warehouseLocation);
    conf.set(InputFormatConfig.CATALOG_NAME, Catalogs.ICEBERG_DEFAULT_CATALOG_NAME);
    conf.set(InputFormatConfig.catalogPropertyConfigKey(Catalogs.ICEBERG_DEFAULT_CATALOG_NAME,
        CatalogUtil.ICEBERG_CATALOG_TYPE), CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP);
    conf.set(InputFormatConfig.catalogPropertyConfigKey(Catalogs.ICEBERG_DEFAULT_CATALOG_NAME,
        CatalogProperties.WAREHOUSE_LOCATION), warehouseLocation);

    Catalog catalog = new HadoopCatalog(conf, conf.get("warehouse.location"));
    TableIdentifier identifier = TableIdentifier.of("db", "t");
    Table table = catalog.createTable(identifier, SCHEMA, SPEC, helper.properties());
    helper.setTable(table);

    List<Record> expectedRecords = helper.generateRandomRecords(1, 0L);
    expectedRecords.get(0).set(2, "2020-03-20");
    helper.appendToTable(Row.of("2020-03-20", 0), expectedRecords);

    builder.readFrom(identifier);

    testInputFormat.create(builder.conf()).validate(expectedRecords);
  }

  @Test
  public void testDeriveLlapSetsCacheAffinityForIcebergInputFormat() {
    MapWork mapWork = new MapWork();
    PartitionDesc partitionDesc = new PartitionDesc();
    partitionDesc.setInputFileFormatClass(HiveIcebergInputFormat.class);
    mapWork.addPathToPartitionInfo(new Path("/tmp"), partitionDesc);
    Configuration job = new Configuration(false);
    HiveConf.setVar(job, HiveConf.ConfVars.LLAP_IO_ENABLED, "true");
    HiveConf.setBoolVar(job, HiveConf.ConfVars.LLAP_IO_NONVECTOR_WRAPPER_ENABLED, true);

    mapWork.setVectorMode(true);
    mapWork.deriveLlap(job, false);

    assertTrue("Cache affinity should be set for HiveIcebergInputFormat when LLAP and vectorization is enabled",
        mapWork.getCacheAffinity());

    mapWork.setVectorMode(false);
    mapWork.deriveLlap(job, false);

    assertFalse("Cache affinity should be disabled for HiveIcebergInputFormat when LLAP is on, but vectorization not",
        mapWork.getCacheAffinity());
  }

  @Test
  public void testWorkerPool() throws Exception {
    Table table = helper.createUnpartitionedTable();
    UserGroupInformation user1 =
            UserGroupInformation.createUserForTesting("user1", new String[] {});
    UserGroupInformation user2 =
            UserGroupInformation.createUserForTesting("user2", new String[] {});
    final ExecutorService workerPool1 = ThreadPools.newFixedThreadPool("iceberg-plan-worker-pool", 1);
    final ExecutorService workerPool2 = ThreadPools.newFixedThreadPool("iceberg-plan-worker-pool", 1);
    try {
      assertThat(getUserFromWorkerPool(user1, table, workerPool1)).isEqualTo("user1");
      assertThat(getUserFromWorkerPool(user2, table, workerPool1)).isEqualTo("user1");
      assertThat(getUserFromWorkerPool(user2, table, workerPool2)).isEqualTo("user2");
    } finally {
      workerPool1.shutdown();
      workerPool2.shutdown();
    }
  }

  private String getUserFromWorkerPool(
          UserGroupInformation user, Table table, ExecutorService workerpool) throws Exception {
    Method method =
            IcebergInputFormat.class.getDeclaredMethod(
                    "planInputSplits", Table.class, Configuration.class, ExecutorService.class);
    method.setAccessible(true);
    return user.doAs(
            (PrivilegedAction<String>)
                    () -> {
                      try {
                        method.invoke(new IcebergInputFormat<>(), table, conf, workerpool);
                        return workerpool
                                .submit(() -> UserGroupInformation.getCurrentUser().getUserName())
                                .get();
                      } catch (Exception e) {
                        throw new RuntimeException("Failed to get user from worker pool", e);
                      }
                    });
  }

  // TODO - Capture template type T in toString method: https://github.com/apache/iceberg/issues/1542
  public abstract static class TestInputFormat<T> {

    private final List<IcebergSplit> splits;
    private final List<T> records;

    private TestInputFormat(List<IcebergSplit> splits, List<T> records) {
      this.splits = splits;
      this.records = records;
    }

    public List<T> getRecords() {
      return records;
    }

    public List<IcebergSplit> getSplits() {
      return splits;
    }

    public void validate(List<T> expected) {
      Assert.assertEquals(expected, records);
    }

    public interface Factory<T> {
      String name();
      TestInputFormat<T> create(Configuration conf);
    }

    public static <T> Factory<T> newFactory(String name, Function<Configuration, TestInputFormat<T>> function) {
      return new Factory<T>() {
        @Override
        public String name() {
          return name;
        }

        @Override
        public TestInputFormat<T> create(Configuration conf) {
          return function.apply(conf);
        }

        @Override
        public String toString() {
          return String.format("Test%s<T>", name());
        }
      };
    }
  }

  private static final class TestMapredIcebergInputFormat<T> extends TestInputFormat<T> {

    private TestMapredIcebergInputFormat(List<IcebergSplit> splits, List<T> records) {
      super(splits, records);
    }

    private static <T> TestMapredIcebergInputFormat<T> create(Configuration conf) {
      JobConf job = new JobConf(conf);
      MapredIcebergInputFormat<T> inputFormat = new MapredIcebergInputFormat<>();

      try {
        org.apache.hadoop.mapred.InputSplit[] splits = inputFormat.getSplits(job, 1);

        List<IcebergSplit> iceSplits = Lists.newArrayListWithExpectedSize(splits.length);
        List<T> records = Lists.newArrayList();

        for (org.apache.hadoop.mapred.InputSplit split : splits) {
          iceSplits.add((IcebergSplit) split);
          org.apache.hadoop.mapred.RecordReader<Void, Container<T>>
                  reader = inputFormat.getRecordReader(split, job, Reporter.NULL);

          try {
            Container<T> container = reader.createValue();

            while (reader.next(null, container)) {
              records.add(container.get());
            }
          } finally {
            reader.close();
          }
        }

        return new TestMapredIcebergInputFormat<>(iceSplits, records);
      } catch (IOException ioe) {
        throw new UncheckedIOException(ioe);
      }
    }
  }

  private static final class TestIcebergInputFormat<T> extends TestInputFormat<T> {

    private TestIcebergInputFormat(List<IcebergSplit> splits, List<T> records) {
      super(splits, records);
    }

    private static <T> TestIcebergInputFormat<T> create(Configuration conf) {
      TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
      IcebergInputFormat<T> inputFormat = new IcebergInputFormat<>();
      List<InputSplit> splits = inputFormat.getSplits(context);

      List<IcebergSplit> iceSplits = Lists.newArrayListWithExpectedSize(splits.size());
      List<T> records = Lists.newArrayList();

      for (InputSplit split : splits) {
        iceSplits.add((IcebergSplit) split);

        try (RecordReader<Void, T> reader = inputFormat.createRecordReader(split, context)) {
          reader.initialize(split, context);

          while (reader.nextKeyValue()) {
            records.add(reader.getCurrentValue());
          }
        } catch (InterruptedException ie) {
          throw new RuntimeException(ie);
        } catch (IOException ioe) {
          throw new UncheckedIOException(ioe);
        }
      }

      return new TestIcebergInputFormat<>(iceSplits, records);
    }
  }
}
