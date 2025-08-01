/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.lang.reflect.*;
import static org.mockito.Mockito.mock;

import com.google.common.collect.Sets;
import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.api.DatabaseType;
import org.apache.hadoop.hive.metastore.api.DeleteColumnStatisticsRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsFilterSpec;
import org.apache.hadoop.hive.metastore.api.GetProjectionsSpec;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsResponse;
import org.apache.hadoop.hive.metastore.api.PartitionSpecWithSharedSD;
import org.apache.hadoop.hive.metastore.api.PartitionWithoutSD;
import org.apache.hadoop.hive.metastore.api.SourceTable;
import org.apache.hadoop.hive.metastore.client.SynchronizedMetaStoreClient;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.dataconnector.jdbc.AbstractJDBCConnectorProvider;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.hadoop.hive.metastore.utils.MetastoreVersionInfo;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.orc.impl.OrcAcidUtils;
import org.datanucleus.api.jdo.JDOPersistenceManager;
import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;
import org.junit.Test;

import com.google.common.collect.Lists;

import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.convertToGetPartitionsByNamesRequest;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public abstract class TestHiveMetaStore {
  private static final Logger LOG = LoggerFactory.getLogger(TestHiveMetaStore.class);
  protected static HiveMetaStoreClient client;
  protected static Configuration conf = null;
  protected static Warehouse warehouse;
  protected static boolean isThriftClient = false;

  private static final String ENGINE = "hive";
  private static final String TEST_DB1_NAME = "testdb1";
  private static final String TEST_DB2_NAME = "testdb2";

  private static final int DEFAULT_LIMIT_PARTITION_REQUEST = 100;

  protected abstract HiveMetaStoreClient createClient() throws Exception;

  @Before
  public void setUp() throws Exception {
    initConf();
    warehouse = new Warehouse(conf);

    // set some values to use for getting conf. vars
    MetastoreConf.setBoolVar(conf, ConfVars.METRICS_ENABLED, true);
    conf.set("hive.key1", "value1");
    conf.set("hive.key2", "http://www.example.com");
    conf.set("hive.key3", "");
    conf.set("hive.key4", "0");
    conf.set("datanucleus.autoCreateTables", "false");
    conf.set("hive.in.test", "true");
    MetastoreConf.setVar(conf, ConfVars.METASTORE_METADATA_TRANSFORMER_CLASS, " ");

    MetaStoreTestUtils.setConfForStandloneMode(conf);
    MetastoreConf.setLongVar(conf, ConfVars.BATCH_RETRIEVE_MAX, 2);
    MetastoreConf.setLongVar(conf, ConfVars.LIMIT_PARTITION_REQUEST, DEFAULT_LIMIT_PARTITION_REQUEST);
    MetastoreConf.setVar(conf, ConfVars.STORAGE_SCHEMA_READER_IMPL, "no.such.class");
    MetastoreConf.setBoolVar(conf, ConfVars.INTEGER_JDO_PUSHDOWN, true);
  }

  protected void initConf() {
    if (null == conf) {
      conf = MetastoreConf.newMetastoreConf();
    }
  }

  @Test
  public void testNameMethods() {
    Map<String, String> spec = new LinkedHashMap<>();
    spec.put("ds", "2008-07-01 14:13:12");
    spec.put("hr", "14");
    List<String> vals = new ArrayList<>();
    vals.addAll(spec.values());
    String partName = "ds=2008-07-01 14%3A13%3A12/hr=14";

    try {
      List<String> testVals = client.partitionNameToVals(partName);
      assertTrue("Values from name are incorrect: " + testVals, vals.equals(testVals));

      Map<String, String> testSpec = client.partitionNameToSpec(partName);
      assertTrue("Spec from name is incorrect: " + testSpec, spec.equals(testSpec));

      List<String> emptyVals = client.partitionNameToVals("");
      assertEquals("Values should be empty", 0, emptyVals.size());

      Map<String, String> emptySpec =  client.partitionNameToSpec("");
      assertEquals("Spec should be empty", 0, emptySpec.size());
    } catch (Exception e) {
      fail();
    }
  }

  /**
   * tests create table and partition and tries to drop the table without
   * droppping the partition
   *
   */
  @Test
  public void testPartition() throws Exception {
    partitionTester(client, conf);
  }

  private static void partitionTester(HiveMetaStoreClient client, Configuration conf)
    throws Exception {
    try {
      String dbName = "compdb";
      String tblName = "comptbl";
      String typeName = "Person";
      List<String> vals = makeVals("2008-07-01 14:13:12", "14");
      List<String> vals2 = makeVals("2008-07-01 14:13:12", "15");
      List<String> vals3 = makeVals("2008-07-02 14:13:12", "15");
      List<String> vals4 = makeVals("2008-07-03 14:13:12", "151");

      client.dropTable(dbName, tblName);
      silentDropDatabase(dbName);
      new DatabaseBuilder()
          .setName(dbName)
          .create(client, conf);
      Database db = client.getDatabase(dbName);
      Path dbPath = new Path(db.getLocationUri());
      FileSystem fs = FileSystem.get(dbPath.toUri(), conf);

      client.dropType(typeName);
      Type typ1 = new Type();
      typ1.setName(typeName);
      typ1.setFields(new ArrayList<>(2));
      typ1.getFields().add(
          new FieldSchema("name", ColumnType.STRING_TYPE_NAME, ""));
      typ1.getFields().add(
          new FieldSchema("income", ColumnType.INT_TYPE_NAME, ""));
      client.createType(typ1);

      List<String> skewedColValue = Collections.singletonList("1");
      Table tbl = new TableBuilder()
          .setDbName(dbName)
          .setTableName(tblName)
          .setCols(typ1.getFields())
          .setNumBuckets(1)
          .addBucketCol("name")
          .addTableParam("test_param_1", "Use this for comments etc")
          .addSerdeParam(ColumnType.SERIALIZATION_FORMAT, "1")
          .addSkewedColName("name")
          .setSkewedColValues(Collections.singletonList(skewedColValue))
          .setSkewedColValueLocationMaps(Collections.singletonMap(skewedColValue, "location1"))
          .addPartCol("ds", ColumnType.STRING_TYPE_NAME)
          .addPartCol("hr", ColumnType.STRING_TYPE_NAME)
          .create(client, conf);

      if (isThriftClient) {
        // the createTable() above does not update the location in the 'tbl'
        // object when the client is a thrift client and the code below relies
        // on the location being present in the 'tbl' object - so get the table
        // from the metastore
        tbl = client.getTable(dbName, tblName);
      }

      Assert.assertTrue(tbl.isSetId());
      tbl.unsetId();

      Partition part = makePartitionObject(dbName, tblName, vals, tbl, "/part1");
      Partition part2 = makePartitionObject(dbName, tblName, vals2, tbl, "/part2");
      Partition part3 = makePartitionObject(dbName, tblName, vals3, tbl, "/part3");
      Partition part4 = makePartitionObject(dbName, tblName, vals4, tbl, "/part4");

      // check if the partition exists (it shouldn't)
      boolean exceptionThrown = false;
      try {
        Partition p = client.getPartition(dbName, tblName, vals);
      } catch(Exception e) {
        assertEquals("partition should not have existed",
            NoSuchObjectException.class, e.getClass());
        exceptionThrown = true;
      }
      assertTrue("getPartition() should have thrown NoSuchObjectException", exceptionThrown);
      Partition retp = client.add_partition(part);
      assertNotNull("Unable to create partition " + part, retp);
      Partition retp2 = client.add_partition(part2);
      assertNotNull("Unable to create partition " + part2, retp2);
      Partition retp3 = client.add_partition(part3);
      assertNotNull("Unable to create partition " + part3, retp3);
      Partition retp4 = client.add_partition(part4);
      assertNotNull("Unable to create partition " + part4, retp4);

      Partition part_get = client.getPartition(dbName, tblName, part.getValues());
      // since we are using thrift, 'part' will not have the create time and
      // last DDL time set since it does not get updated in the add_partition()
      // call - likewise part2 and part3 - set it correctly so that equals check
      // doesn't fail
      adjust(client, part, dbName, tblName, isThriftClient);
      adjust(client, part2, dbName, tblName, isThriftClient);
      adjust(client, part3, dbName, tblName, isThriftClient);
      assertTrue("Partitions are not same, got: " + part_get, part.equals(part_get));

      // check null cols schemas for a partition
      List<String> vals6 = makeVals("2016-02-22 00:00:00", "16");
      Partition part6 = makePartitionObject(dbName, tblName, vals6, tbl, "/part5");
      part6.getSd().setCols(null);
      LOG.info("Creating partition will null field schema");
      client.add_partition(part6);
      LOG.info("Listing all partitions for table " + dbName + "." + tblName);
      final List<Partition> partitions = client.listPartitions(dbName, tblName, (short) -1);
      boolean foundPart = false;
      for (Partition p : partitions) {
        if (p.getValues().equals(vals6)) {
          assertNull(p.getSd().getCols());
          LOG.info("Found partition " + p + " having null field schema");
          foundPart = true;
        }
      }
      assertTrue(foundPart);

      String partName = "ds=" + FileUtils.escapePathName("2008-07-01 14:13:12") + "/hr=14";
      String part2Name = "ds=" + FileUtils.escapePathName("2008-07-01 14:13:12") + "/hr=15";
      String part3Name = "ds=" + FileUtils.escapePathName("2008-07-02 14:13:12") + "/hr=15";
      String part4Name = "ds=" + FileUtils.escapePathName("2008-07-03 14:13:12") + "/hr=151";

      part_get = client.getPartition(dbName, tblName, partName);
      assertTrue("Partitions are not the same", part.equals(part_get));

      // Test partition listing with a partial spec - ds is specified but hr is not
      List<String> partialVals = new ArrayList<>();
      partialVals.add(vals.get(0));
      Set<Partition> parts = new HashSet<>();
      parts.add(part);
      parts.add(part2);

      List<Partition> partial = client.listPartitions(dbName, tblName, partialVals,
          (short) -1);
      assertEquals("Should have returned 2 partitions", 2, partial.size());
      assertTrue("Not all parts returned", partial.containsAll(parts));

      Set<String> partNames = new HashSet<>();
      partNames.add(partName);
      partNames.add(part2Name);
      List<String> partialNames = client.listPartitionNames(dbName, tblName, partialVals,
          (short) -1);
      assertEquals("Should have returned 2 partition names", 2, partialNames.size());
      assertTrue("Not all part names returned", partialNames.containsAll(partNames));

      partNames.add(part3Name);
      partNames.add(part4Name);
      partialVals.clear();
      partialVals.add("");
      partialNames = client.listPartitionNames(dbName, tblName, partialVals, (short) -1);
      assertEquals("Should have returned 5 partition names", 5, partialNames.size());
      assertTrue("Not all part names returned", partialNames.containsAll(partNames));

      // Test partition listing with a partial spec - hr is specified but ds is not
      parts.clear();
      parts.add(part2);
      parts.add(part3);

      partialVals.clear();
      partialVals.add("");
      partialVals.add(vals2.get(1));

      partial = client.listPartitions(dbName, tblName, partialVals, (short) -1);
      assertEquals("Should have returned 2 partitions", 2, partial.size());
      assertTrue("Not all parts returned", partial.containsAll(parts));

      partNames.clear();
      partNames.add(part2Name);
      partNames.add(part3Name);
      partialNames = client.listPartitionNames(dbName, tblName, partialVals,
          (short) -1);
      assertEquals("Should have returned 2 partition names", 2, partialNames.size());
      assertTrue("Not all part names returned", partialNames.containsAll(partNames));

      // Verify escaped partition names don't return partitions
      exceptionThrown = false;
      try {
        String badPartName = "ds=2008-07-01 14%3A13%3A12/hrs=14";
        client.getPartition(dbName, tblName, badPartName);
      } catch(NoSuchObjectException e) {
        exceptionThrown = true;
      }
      assertTrue("Bad partition spec should have thrown an exception", exceptionThrown);

      Path partPath = new Path(part.getSd().getLocation());


      assertTrue(partPath + " doesn't exist", fs.exists(partPath));
      client.dropPartition(dbName, tblName, part.getValues(), true);
      assertFalse(partPath + " still exists", fs.exists(partPath));

      // Test append_partition_by_name
      client.appendPartition(dbName, tblName, partName);
      Partition part5 = client.getPartition(dbName, tblName, part.getValues());
      assertTrue("Append partition by name failed", part5.getValues().equals(vals));
      Path part5Path = new Path(part5.getSd().getLocation());
      assertTrue(part5Path + " doesn't exist", fs.exists(part5Path));

      // Test drop_partition_by_name
      assertTrue("Drop partition by name failed",
          client.dropPartition(dbName, tblName, partName, true));
      assertFalse(part5Path + " still exists", fs.exists(part5Path));

      // add the partition again so that drop table with a partition can be
      // tested
      retp = client.add_partition(part);
      assertNotNull("Unable to create partition " + part, retp);

      // test add_partitions

      List<String> mvals1 = makeVals("2008-07-04 14:13:12", "14641");
      List<String> mvals2 = makeVals("2008-07-04 14:13:12", "14642");
      List<String> mvals3 = makeVals("2008-07-04 14:13:12", "14643");
      List<String> mvals4 = makeVals("2008-07-04 14:13:12", "14643"); // equal to 3
      List<String> mvals5 = makeVals("2008-07-04 14:13:12", "14645");

      Exception savedException;

      // add_partitions(empty list) : ok, normal operation
      client.add_partitions(new ArrayList<>());

      // add_partitions(1,2,3) : ok, normal operation
      Partition mpart1 = makePartitionObject(dbName, tblName, mvals1, tbl, "/mpart1");
      Partition mpart2 = makePartitionObject(dbName, tblName, mvals2, tbl, "/mpart2");
      Partition mpart3 = makePartitionObject(dbName, tblName, mvals3, tbl, "/mpart3");
      client.add_partitions(Arrays.asList(mpart1,mpart2,mpart3));

      // do DDL time munging if thrift mode
      adjust(client, mpart1, dbName, tblName, isThriftClient);
      adjust(client, mpart2, dbName, tblName, isThriftClient);
      adjust(client, mpart3, dbName, tblName, isThriftClient);
      verifyPartitionsPublished(client, dbName, tblName,
          Arrays.asList(mvals1.get(0)),
          Arrays.asList(mpart1,mpart2,mpart3));

      Partition mpart4 = makePartitionObject(dbName, tblName, mvals4, tbl, "/mpart4");
      Partition mpart5 = makePartitionObject(dbName, tblName, mvals5, tbl, "/mpart5");

      // create dir for /mpart5
      Path mp5Path = new Path(mpart5.getSd().getLocation());
      warehouse.mkdirs(mp5Path);
      assertTrue(mp5Path + " doesn't exist", fs.exists(mp5Path));

      // add_partitions(5,4) : err = duplicate keyvals on mpart4
      savedException = null;
      try {
        client.add_partitions(Arrays.asList(mpart5,mpart4));
      } catch (Exception e) {
        savedException = e;
      } finally {
        assertNotNull(savedException);
      }

      // check that /mpart4 does not exist, but /mpart5 still does.
      assertTrue(mp5Path + " doesn't exist", fs.exists(mp5Path));
      Path mp4Path = new Path(mpart4.getSd().getLocation());
      assertFalse(mp4Path + " exists", fs.exists(mp4Path));

      // add_partitions(5) : ok
      client.add_partitions(Arrays.asList(mpart5));

      // do DDL time munging if thrift mode
      adjust(client, mpart5, dbName, tblName, isThriftClient);

      verifyPartitionsPublished(client, dbName, tblName,
          Arrays.asList(mvals1.get(0)),
          Arrays.asList(mpart1,mpart2,mpart3,mpart5));

      //// end add_partitions tests

      client.dropTable(dbName, tblName);

      client.dropType(typeName);

      // recreate table as external, drop partition and it should
      // still exist
      tbl.setParameters(new HashMap<>());
      tbl.getParameters().put("EXTERNAL", "TRUE");
      client.createTable(tbl);
      retp = client.add_partition(part);
      assertTrue(partPath + " doesn't exist", fs.exists(partPath));
      client.dropPartition(dbName, tblName, part.getValues(), true);
      assertTrue(partPath + " still exists", fs.exists(partPath));

      for (String tableName : client.getTables(dbName, "*")) {
        client.dropTable(dbName, tableName);
      }

      client.dropDatabase(dbName);

    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testPartition() failed.");
      throw e;
    }
  }

  private static void verifyPartitionsPublished(HiveMetaStoreClient client,
      String dbName, String tblName, List<String> partialSpec,
      List<Partition> expectedPartitions) throws TException {
    // Test partition listing with a partial spec

    List<Partition> mpartial = client.listPartitions(dbName, tblName, partialSpec,
        (short) -1);
    assertEquals("Should have returned "+expectedPartitions.size()+
        " partitions, returned " + mpartial.size(),
        expectedPartitions.size(), mpartial.size());
    assertTrue("Not all parts returned", mpartial.containsAll(expectedPartitions));
  }

  public static List<String> makeVals(String ds, String id) {
    List <String> vals4 = new ArrayList<>(2);
    vals4.add(ds);
    vals4.add(id);
    return vals4;
  }

  private static Partition makePartitionObject(String dbName, String tblName,
      List<String> ptnVals, Table tbl, String ptnLocationSuffix) throws MetaException {
    String absoluteLocation = tbl.getSd().getLocation() + ptnLocationSuffix;
    return makePartitionObjectWithAbsoluteLocation(dbName, tblName, ptnVals, tbl, absoluteLocation);
  }

  private static Partition makePartitionObjectWithAbsoluteLocation(String dbName, String tblName,
      List<String> ptnVals, Table tbl, String absolutePartitionLocation) throws MetaException {
    Partition part = new Partition();
    part.setDbName(dbName);
    part.setTableName(tblName);
    part.setValues(ptnVals);
    part.setParameters(new HashMap<>());
    part.setSd(tbl.getSd().deepCopy());
    part.getSd().setSerdeInfo(tbl.getSd().getSerdeInfo().deepCopy());
    part.getSd().setLocation(absolutePartitionLocation);
    MetaStoreServerUtils.updatePartitionStatsFast(part, tbl, warehouse, false, false, null, true);
    return part;
  }

  @Test
  public void testListPartitions() throws Throwable {
    // create a table with multiple partitions
    String dbName = "compdb";
    String tblName = "comptbl";
    String typeName = "Person";

    cleanUp(dbName, tblName, typeName);

    List<List<String>> values = new ArrayList<>();
    values.add(makeVals("2008-07-01 14:13:12", "14"));
    values.add(makeVals("2008-07-01 14:13:12", "15"));
    values.add(makeVals("2008-07-02 14:13:12", "15"));
    values.add(makeVals("2008-07-03 14:13:12", "151"));

    createMultiPartitionTableSchema(dbName, tblName, typeName, values);

    List<Partition> partitions = client.listPartitions(dbName, tblName, (short)-1);
    assertNotNull("should have returned partitions", partitions);
    assertEquals(" should have returned " + values.size() +
      " partitions", values.size(), partitions.size());

    partitions = client.listPartitions(dbName, tblName, (short)(values.size()/2));

    assertNotNull("should have returned partitions", partitions);
    assertEquals(" should have returned " + values.size() / 2 +
      " partitions",values.size() / 2, partitions.size());


    partitions = client.listPartitions(dbName, tblName, (short) (values.size() * 2));

    assertNotNull("should have returned partitions", partitions);
    assertEquals(" should have returned " + values.size() +
      " partitions",values.size(), partitions.size());

    cleanUp(dbName, tblName, typeName);
  }

  @Test
  public void testListPartitionsWihtLimitEnabled() throws Throwable {
    // create a table with multiple partitions
    String dbName = "compdb";
    String tblName = "comptbl";
    String typeName = "Person";
    String ds = "2008-07-01 14:13:12";

    cleanUp(dbName, tblName, typeName);

    // Create too many partitions, just enough to validate over limit requests
    List<List<String>> values = new ArrayList<>();
    for (int i=0; i<DEFAULT_LIMIT_PARTITION_REQUEST + 1; i++) {
      values.add(makeVals(ds, Integer.toString(i)));
    }

    createMultiPartitionTableSchema(dbName, tblName, typeName, values);

    List<Partition> partitions;
    List<String> partVal = Collections.singletonList(ds);
    short maxParts;

    // Requesting more partitions than allowed should throw an exception
    maxParts = -1;
    try {
      partitions = client.listPartitions(dbName, tblName, maxParts);
      fail("should have thrown MetaException about partition limit");
    } catch (MetaException e) {
      assertTrue(true);
    }

    try {
      client.listPartitions(dbName, tblName, partVal, maxParts);
      fail("should have thrown MetaException about partition limit");
    } catch (MetaException e) {
      assertTrue(true);
    }

    // Requesting more partitions than allowed should throw an exception
    maxParts = DEFAULT_LIMIT_PARTITION_REQUEST + 1;
    try {
      partitions = client.listPartitions(dbName, tblName, maxParts);
      fail("should have thrown MetaException about partition limit");
    } catch (MetaException e) {
      assertTrue(true);
    }

    try {
      client.listPartitions(dbName, tblName, partVal, maxParts);
      fail("should have thrown MetaException about partition limit");
    } catch (MetaException e) {
      assertTrue(true);
    }

    // Requesting less partitions than allowed should work
    maxParts = DEFAULT_LIMIT_PARTITION_REQUEST / 2;
    partitions = client.listPartitions(dbName, tblName, maxParts);
    assertNotNull("should have returned partitions", partitions);
    assertEquals(" should have returned 50 partitions", maxParts, partitions.size());

    partitions = client.listPartitions(dbName, tblName, partVal, maxParts);
    assertNotNull("should have returned partitions", partitions);
    assertEquals(" should have returned 50 partitions", maxParts, partitions.size());
  }

  @Test
  public void testAlterTableCascade() throws Throwable {
    // create a table with multiple partitions
    String dbName = "compdb";
    String tblName = "comptbl";
    String typeName = "Person";

    cleanUp(dbName, tblName, typeName);

    List<List<String>> values = new ArrayList<>();
    values.add(makeVals("2008-07-01 14:13:12", "14"));
    values.add(makeVals("2008-07-01 14:13:12", "15"));
    values.add(makeVals("2008-07-02 14:13:12", "15"));
    values.add(makeVals("2008-07-03 14:13:12", "151"));

    createMultiPartitionTableSchema(dbName, tblName, typeName, values);
    Table tbl = client.getTable(dbName, tblName);
    List<FieldSchema> cols = tbl.getSd().getCols();
    cols.add(new FieldSchema("new_col", ColumnType.STRING_TYPE_NAME, ""));
    tbl.getSd().setCols(cols);
    //add new column with cascade option
    client.alter_table(dbName, tblName, tbl, true);
    //
    Table tbl2 = client.getTable(dbName, tblName);
    assertEquals("Unexpected number of cols", 3, tbl2.getSd().getCols().size());
    assertEquals("Unexpected column name", "new_col", tbl2.getSd().getCols().get(2).getName());
    //get a partition
    List<String> pvalues = new ArrayList<>(2);
    pvalues.add("2008-07-01 14:13:12");
    pvalues.add("14");
    Partition partition = client.getPartition(dbName, tblName, pvalues);
    assertEquals("Unexpected number of cols", 3, partition.getSd().getCols().size());
    assertEquals("Unexpected column name", "new_col", partition.getSd().getCols().get(2).getName());

    //add another column
    cols = tbl.getSd().getCols();
    cols.add(new FieldSchema("new_col2", ColumnType.STRING_TYPE_NAME, ""));
    tbl.getSd().setCols(cols);
    //add new column with no cascade option
    client.alter_table(dbName, tblName, tbl, false);
    tbl2 = client.getTable(dbName, tblName);
    assertEquals("Unexpected number of cols", 4, tbl2.getSd().getCols().size());
    assertEquals("Unexpected column name", "new_col2", tbl2.getSd().getCols().get(3).getName());
    //get partition, this partition should not have the newly added column since cascade option
    //was false
    partition = client.getPartition(dbName, tblName, pvalues);
    assertEquals("Unexpected number of cols", 3, partition.getSd().getCols().size());
  }


  @Test
  public void testListPartitionNames() throws Throwable {
    // create a table with multiple partitions
    String dbName = "compdb";
    String tblName = "comptbl";
    String typeName = "Person";

    cleanUp(dbName, tblName, typeName);

    List<List<String>> values = new ArrayList<>();
    values.add(makeVals("2008-07-01 14:13:12", "14"));
    values.add(makeVals("2008-07-01 14:13:12", "15"));
    values.add(makeVals("2008-07-02 14:13:12", "15"));
    values.add(makeVals("2008-07-03 14:13:12", "151"));



    createMultiPartitionTableSchema(dbName, tblName, typeName, values);

    List<String> partitions = client.listPartitionNames(dbName, tblName, (short)-1);
    assertNotNull("should have returned partitions", partitions);
    assertEquals(" should have returned " + values.size() +
      " partitions", values.size(), partitions.size());

    partitions = client.listPartitionNames(dbName, tblName, (short)(values.size()/2));

    assertNotNull("should have returned partitions", partitions);
    assertEquals(" should have returned " + values.size() / 2 +
      " partitions",values.size() / 2, partitions.size());


    partitions = client.listPartitionNames(dbName, tblName, (short) (values.size() * 2));

    assertNotNull("should have returned partitions", partitions);
    assertEquals(" should have returned " + values.size() +
      " partitions",values.size(), partitions.size());

    cleanUp(dbName, tblName, typeName);

  }

  @Test
  public void testGetPartitionsWithSpec() throws Throwable {
    // create a table with multiple partitions
    List<Partition> createdPartitions = setupProjectionTestTable();
    Table tbl = client.getTable("compdb", "comptbl");
    GetPartitionsRequest request = new GetPartitionsRequest();
    GetProjectionsSpec projectSpec = new GetProjectionsSpec();
    projectSpec.setFieldList(Arrays
        .asList("dbName", "tableName", "catName", "parameters", "lastAccessTime", "sd.location",
            "values", "createTime", "sd.serdeInfo.serializationLib", "sd.cols"));
    projectSpec.setExcludeParamKeyPattern("exclude%");
    GetPartitionsFilterSpec filter = new GetPartitionsFilterSpec();
    request.setDbName("compdb");
    request.setTblName("comptbl");
    request.setFilterSpec(filter);
    request.setProjectionSpec(projectSpec);
    GetPartitionsResponse response;
    try {
      response = client.getPartitionsWithSpecs(request);
    } catch (Exception ex) {
      ex.printStackTrace();
      LOG.error("Exception while retriveing partitions", ex);
      throw ex;
    }

    Assert.assertEquals(1, response.getPartitionSpecSize());
    PartitionSpecWithSharedSD partitionSpecWithSharedSD =
        response.getPartitionSpec().get(0).getSharedSDPartitionSpec();
    Assert.assertNotNull(partitionSpecWithSharedSD.getSd());
    StorageDescriptor sharedSD = partitionSpecWithSharedSD.getSd();
    Assert.assertEquals("Root location should be set to table location", tbl.getSd().getLocation(),
        sharedSD.getLocation());
    Assert.assertFalse("Fields which are not requested should not be set",
        sharedSD.isSetParameters());
    Assert.assertNotNull(
        "serializationLib class was requested but was not found in the returned partition",
        sharedSD.getSerdeInfo().getSerializationLib());
    Assert.assertNotNull("db name was requested but was not found in the returned partition",
        response.getPartitionSpec().get(0).getDbName());
    Assert.assertNotNull("Table name was requested but was not found in the returned partition",
        response.getPartitionSpec().get(0).getTableName());
    Assert.assertTrue("sd.cols was requested but was not found in the returned response",
        partitionSpecWithSharedSD.getSd().isSetCols());
    List<FieldSchema> origSdCols = createdPartitions.get(0).getSd().getCols();
    Assert.assertEquals("Size of the requested sd.cols should be same", origSdCols.size(),
        partitionSpecWithSharedSD.getSd().getCols().size());
    for (int i = 0; i < origSdCols.size(); i++) {
      FieldSchema origFs = origSdCols.get(i);
      FieldSchema returnedFs = partitionSpecWithSharedSD.getSd().getCols().get(i);
      Assert.assertEquals("Field schemas returned different than expected", origFs, returnedFs);
    }
    /*Assert
        .assertNotNull("Catalog name was requested but was not found in the returned partition",
            response.getPartitionSpec().get(0).getCatName());*/

    List<PartitionWithoutSD> partitionWithoutSDS = partitionSpecWithSharedSD.getPartitions();
    Assert.assertEquals(createdPartitions.size(), partitionWithoutSDS.size());
    for (int i = 0; i < createdPartitions.size(); i++) {
      Partition origPartition = createdPartitions.get(i);
      PartitionWithoutSD returnedPartitionWithoutSD = partitionWithoutSDS.get(i);
      Assert.assertEquals(String.format("Location returned for Partition %d is not correct", i),
          origPartition.getSd().getLocation(),
          sharedSD.getLocation() + returnedPartitionWithoutSD.getRelativePath());
      Assert.assertTrue("createTime was request but is not set",
          returnedPartitionWithoutSD.isSetCreateTime());
      Assert.assertTrue("Partition parameters were requested but are not set",
          returnedPartitionWithoutSD.isSetParameters());
      // first partition has parameters set
      if (i == 0) {
        Assert.assertTrue("partition parameters not set",
            returnedPartitionWithoutSD.getParameters().containsKey("key1"));
        Assert.assertEquals("partition parameters does not contain included keys", "val1",
            returnedPartitionWithoutSD.getParameters().get("key1"));
        // excluded parameter should not be returned
        Assert.assertFalse("Excluded parameter key returned",
            returnedPartitionWithoutSD.getParameters().containsKey("excludeKey1"));
        Assert.assertFalse("Excluded parameter key returned",
            returnedPartitionWithoutSD.getParameters().containsKey("excludeKey2"));
      }
      List<String> returnedVals = returnedPartitionWithoutSD.getValues();
      List<String> actualVals = origPartition.getValues();
      for (int j = 0; j < actualVals.size(); j++) {
        Assert.assertEquals(actualVals.get(j), returnedVals.get(j));
      }
    }
  }


  protected List<Partition> setupProjectionTestTable() throws Throwable {
    //String catName = "catName";
    String dbName = "compdb";
    String tblName = "comptbl";
    String typeName = "Person";
    //String catName = "catName";
    Map<String, String> dummyparams = new HashMap<>();
    dummyparams.put("key1", "val1");
    dummyparams.put("excludeKey1", "excludeVal1");
    dummyparams.put("excludeKey2", "excludeVal2");
    cleanUp(dbName, tblName, typeName);

    List<List<String>> values = new ArrayList<>();
    values.add(makeVals("2008-07-01 14:13:12", "14"));
    values.add(makeVals("2008-07-01 14:13:12", "15"));
    values.add(makeVals("2008-07-02 14:13:12", "15"));
    values.add(makeVals("2008-07-03 14:13:12", "151"));

    List<Partition> createdPartitions =
        createMultiPartitionTableSchema(dbName, tblName, typeName, values);
    Table tbl = client.getTable(dbName, tblName);
    // add some dummy parameters to one of the partitions to confirm the fetching logic is working
    Partition newPartition = createdPartitions.remove(0);
    //Map<String, String> sdParams = new HashMap<>();
    //dummyparams.put("sdkey1", "sdval1");
    newPartition.setParameters(dummyparams);
    //newPartition.getSd().setParameters(sdParams);

    client.alter_partition(dbName, tblName, newPartition);
    createdPartitions.add(0, newPartition);
    return createdPartitions;
  }

  @Test
  public void testDropTable() throws Throwable {
    // create a table with multiple partitions
    String dbName = "compdb";
    String tblName = "comptbl";
    String typeName = "Person";

    cleanUp(dbName, tblName, typeName);

    List<List<String>> values = new ArrayList<>();
    values.add(makeVals("2008-07-01 14:13:12", "14"));
    values.add(makeVals("2008-07-01 14:13:12", "15"));
    values.add(makeVals("2008-07-02 14:13:12", "15"));
    values.add(makeVals("2008-07-03 14:13:12", "151"));

    createMultiPartitionTableSchema(dbName, tblName, typeName, values);

    client.dropTable(dbName, tblName);
    client.dropType(typeName);

    boolean exceptionThrown = false;
    try {
      client.getTable(dbName, tblName);
    } catch(Exception e) {
      assertEquals("table should not have existed",
          NoSuchObjectException.class, e.getClass());
      exceptionThrown = true;
    }
    assertTrue("Table " + tblName + " should have been dropped ", exceptionThrown);

  }

  @Test
  public void testAlterViewParititon() throws Throwable {
    String dbName = "compdb";
    String tblName = "comptbl";
    String viewName = "compView";

    client.dropTable(dbName, tblName);
    silentDropDatabase(dbName);
    new DatabaseBuilder()
        .setName(dbName)
        .setDescription("Alter Partition Test database")
        .create(client, conf);

    Table tbl = new TableBuilder()
        .setDbName(dbName)
        .setTableName(tblName)
        .addCol("name", ColumnType.STRING_TYPE_NAME)
        .addCol("income", ColumnType.INT_TYPE_NAME)
        .create(client, conf);

    if (isThriftClient) {
      // the createTable() above does not update the location in the 'tbl'
      // object when the client is a thrift client and the code below relies
      // on the location being present in the 'tbl' object - so get the table
      // from the metastore
      tbl = client.getTable(dbName, tblName);
    }

    ArrayList<FieldSchema> viewCols = new ArrayList<>(1);
    viewCols.add(new FieldSchema("income", ColumnType.INT_TYPE_NAME, ""));

    ArrayList<FieldSchema> viewPartitionCols = new ArrayList<>(1);
    viewPartitionCols.add(new FieldSchema("name", ColumnType.STRING_TYPE_NAME, ""));

    Table view = new Table();
    view.setDbName(dbName);
    view.setTableName(viewName);
    view.setTableType(TableType.VIRTUAL_VIEW.name());
    view.setPartitionKeys(viewPartitionCols);
    view.setViewOriginalText("SELECT income, name FROM " + tblName);
    view.setViewExpandedText("SELECT `" + tblName + "`.`income`, `" + tblName +
        "`.`name` FROM `" + dbName + "`.`" + tblName + "`");
    view.setRewriteEnabled(false);
    StorageDescriptor viewSd = new StorageDescriptor();
    view.setSd(viewSd);
    viewSd.setCols(viewCols);
    viewSd.setCompressed(false);
    viewSd.setParameters(new HashMap<>());
    viewSd.setSerdeInfo(new SerDeInfo());
    viewSd.getSerdeInfo().setParameters(new HashMap<>());

    client.createTable(view);

    if (isThriftClient) {
      // the createTable() above does not update the location in the 'tbl'
      // object when the client is a thrift client and the code below relies
      // on the location being present in the 'tbl' object - so get the table
      // from the metastore
      view = client.getTable(dbName, viewName);
    }

    List<String> vals = new ArrayList<>(1);
    vals.add("abc");

    Partition part = new Partition();
    part.setDbName(dbName);
    part.setTableName(viewName);
    part.setValues(vals);
    part.setParameters(new HashMap<>());

    client.add_partition(part);

    Partition part2 = client.getPartition(dbName, viewName, part.getValues());

    part2.getParameters().put("a", "b");

    client.alter_partition(dbName, viewName, part2, null);

    Partition part3 = client.getPartition(dbName, viewName, part.getValues());
    assertEquals("couldn't view alter partition", part3.getParameters().get(
        "a"), "b");

    client.dropTable(dbName, viewName);

    client.dropTable(dbName, tblName);

    client.dropDatabase(dbName);
  }

  @Test
  public void testAlterPartition() throws Throwable {

    try {
      String dbName = "compdb";
      String tblName = "comptbl";
      List<String> vals = new ArrayList<>(2);
      vals.add("2008-07-01");
      vals.add("14");

      client.dropTable(dbName, tblName);
      silentDropDatabase(dbName);
      new DatabaseBuilder()
          .setName(dbName)
          .setDescription("Alter Partition Test database")
          .create(client, conf);

      Table tbl = new TableBuilder()
          .setDbName(dbName)
          .setTableName(tblName)
          .addCol("name", ColumnType.STRING_TYPE_NAME)
          .addCol("income", ColumnType.INT_TYPE_NAME)
          .addTableParam("test_param_1", "Use this for comments etc")
          .addBucketCol("name")
          .addSerdeParam(ColumnType.SERIALIZATION_FORMAT, "1")
          .addPartCol("ds", ColumnType.STRING_TYPE_NAME)
          .addPartCol("hr", ColumnType.INT_TYPE_NAME)
          .create(client, conf);

      if (isThriftClient) {
        // the createTable() above does not update the location in the 'tbl'
        // object when the client is a thrift client and the code below relies
        // on the location being present in the 'tbl' object - so get the table
        // from the metastore
        tbl = client.getTable(dbName, tblName);
      }

      Partition part = new Partition();
      part.setDbName(dbName);
      part.setTableName(tblName);
      part.setValues(vals);
      part.setParameters(new HashMap<>());
      part.setSd(tbl.getSd());
      part.getSd().setSerdeInfo(tbl.getSd().getSerdeInfo());
      part.getSd().setLocation(tbl.getSd().getLocation() + "/part1");

      client.add_partition(part);

      Partition part2 = client.getPartition(dbName, tblName, part.getValues());

      part2.getParameters().put("retention", "10");
      part2.getSd().setNumBuckets(12);
      part2.getSd().getSerdeInfo().getParameters().put("abc", "1");
      client.alter_partition(dbName, tblName, part2, null);

      Partition part3 = client.getPartition(dbName, tblName, part.getValues());
      assertEquals("couldn't alter partition", part3.getParameters().get(
          "retention"), "10");
      assertEquals("couldn't alter partition", part3.getSd().getSerdeInfo()
          .getParameters().get("abc"), "1");
      assertEquals("couldn't alter partition", part3.getSd().getNumBuckets(),
          12);

      client.dropTable(dbName, tblName);

      client.dropDatabase(dbName);
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testPartition() failed.");
      throw e;
    }
  }

  @Test
  public void testRenamePartition() throws Throwable {

    try {
      String dbName = "compdb1";
      String tblName = "comptbl1";
      List<String> vals = new ArrayList<>(2);
      vals.add("2011-07-11");
      vals.add("8");
      String part_path = "/ds=2011-07-11/hr=8";
      List<String> tmp_vals = new ArrayList<>(2);
      tmp_vals.add("tmp_2011-07-11");
      tmp_vals.add("-8");
      String part2_path = "/ds=tmp_2011-07-11/hr=-8";

      client.dropTable(dbName, tblName);
      silentDropDatabase(dbName);
      new DatabaseBuilder()
          .setName(dbName)
          .setDescription("Rename Partition Test database")
          .create(client, conf);

      Table tbl = new TableBuilder()
          .setDbName(dbName)
          .setTableName(tblName)
          .addCol("name", ColumnType.STRING_TYPE_NAME)
          .addCol("income", ColumnType.INT_TYPE_NAME)
          .addPartCol("ds", ColumnType.STRING_TYPE_NAME)
          .addPartCol("hr", ColumnType.INT_TYPE_NAME)
          .create(client, conf);

      if (isThriftClient) {
        // the createTable() above does not update the location in the 'tbl'
        // object when the client is a thrift client and the code below relies
        // on the location being present in the 'tbl' object - so get the table
        // from the metastore
        tbl = client.getTable(dbName, tblName);
      }

      Partition part = new Partition();
      part.setDbName(dbName);
      part.setTableName(tblName);
      part.setValues(vals);
      part.setParameters(new HashMap<>());
      part.setSd(tbl.getSd().deepCopy());
      part.getSd().setSerdeInfo(tbl.getSd().getSerdeInfo());
      part.getSd().setLocation(tbl.getSd().getLocation() + "/part1");
      part.getParameters().put("retention", "10");
      part.getSd().setNumBuckets(12);
      part.getSd().getSerdeInfo().getParameters().put("abc", "1");

      client.add_partition(part);

      part.setValues(tmp_vals);
      client.renamePartition(dbName, tblName, vals, part);

      boolean exceptionThrown = false;
      try {
        Partition p = client.getPartition(dbName, tblName, vals);
      } catch(Exception e) {
        assertEquals("partition should not have existed",
            NoSuchObjectException.class, e.getClass());
        exceptionThrown = true;
      }
      assertTrue("Expected NoSuchObjectException", exceptionThrown);

      Partition part3 = client.getPartition(dbName, tblName, tmp_vals);
      assertEquals("couldn't rename partition", part3.getParameters().get(
          "retention"), "10");
      assertEquals("couldn't rename partition", part3.getSd().getSerdeInfo()
          .getParameters().get("abc"), "1");
      assertEquals("couldn't rename partition", part3.getSd().getNumBuckets(),
          12);
      assertEquals("new partition sd matches", part3.getSd().getLocation(),
          tbl.getSd().getLocation() + part2_path);

      part.setValues(vals);
      client.renamePartition(dbName, tblName, tmp_vals, part);

      exceptionThrown = false;
      try {
        Partition p = client.getPartition(dbName, tblName, tmp_vals);
      } catch(Exception e) {
        assertEquals("partition should not have existed",
            NoSuchObjectException.class, e.getClass());
        exceptionThrown = true;
      }
      assertTrue("Expected NoSuchObjectException", exceptionThrown);

      part3 = client.getPartition(dbName, tblName, vals);
      assertEquals("couldn't rename partition", part3.getParameters().get(
          "retention"), "10");
      assertEquals("couldn't rename partition", part3.getSd().getSerdeInfo()
          .getParameters().get("abc"), "1");
      assertEquals("couldn't rename partition", part3.getSd().getNumBuckets(),
          12);
      assertEquals("new partition sd matches", part3.getSd().getLocation(),
          tbl.getSd().getLocation() + part_path);

      client.dropTable(dbName, tblName);

      client.dropDatabase(dbName);
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testRenamePartition() failed.");
      throw e;
    }
  }

  @Test(expected = InvalidObjectException.class)
  public void testDropTableFetchPartitions() throws Throwable {
    String dbName = "fetchPartitionsDb";
    String tblName = "fetchPartitionsTbl";
    List<String> vals = new ArrayList<>(2);
    vals.add("2011-07-11");
    vals.add("8");
    client.dropTable(dbName, tblName);
    silentDropDatabase(dbName);
    new DatabaseBuilder()
            .setName(dbName)
            .setDescription("Drop table Fetch partition Test database")
            .create(client, conf);

    Table tbl = new TableBuilder()
            .setDbName(dbName)
            .setTableName(tblName)
            .addCol("name", ColumnType.STRING_TYPE_NAME)
            .addCol("income", ColumnType.INT_TYPE_NAME)
            .addPartCol("ds", ColumnType.STRING_TYPE_NAME)
            .addPartCol("hr", ColumnType.INT_TYPE_NAME)
            .create(client, conf);

    if (isThriftClient) {
      // the createTable() above does not update the location in the 'tbl'
      // object when the client is a thrift client and the code below relies
      // on the location being present in the 'tbl' object - so get the table
      // from the metastore
      tbl = client.getTable(dbName, tblName);
    }

    Partition part = new Partition();
    part.setDbName(dbName);
    part.setTableName(tblName);
    part.setValues(vals);
    part.setParameters(new HashMap<>());
    part.setSd(tbl.getSd().deepCopy());
    part.getSd().setLocation(tbl.getSd().getLocation() + "/part1");

    client.add_partition(part);

    GetPartitionsByNamesRequest req = convertToGetPartitionsByNamesRequest(dbName, tblName, vals);
    client.dropTable(dbName, tblName, true, false);
    List<Partition> partitionsList = client.getPartitionsByNames(req).getPartitions();
  }

  @Test
  public void testDatabase() throws Throwable {
    try {
      // clear up any existing databases
      silentDropDatabase(TEST_DB1_NAME);
      silentDropDatabase(TEST_DB2_NAME);

      Database db = new DatabaseBuilder()
          .setName(TEST_DB1_NAME)
          .setOwnerName(SecurityUtils.getUser())
          .build(conf);
      Assert.assertEquals(SecurityUtils.getUser(), db.getOwnerName());
      client.createDatabase(db);

      db = client.getDatabase(TEST_DB1_NAME);

      assertEquals("name of returned db is different from that of inserted db",
          TEST_DB1_NAME, db.getName());
      assertEquals("location of the returned db is different from that of inserted db",
          warehouse.getDatabasePath(db).toString(), db.getLocationUri());
      assertEquals(db.getOwnerName(), SecurityUtils.getUser());
      assertEquals(db.getOwnerType(), PrincipalType.USER);
      assertEquals(Warehouse.DEFAULT_CATALOG_NAME, db.getCatalogName());
      Database db2 = new DatabaseBuilder()
          .setName(TEST_DB2_NAME)
          .create(client, conf);

      db2 = client.getDatabase(TEST_DB2_NAME);

      assertEquals("name of returned db is different from that of inserted db",
          TEST_DB2_NAME, db2.getName());
      assertEquals("location of the returned db is different from that of inserted db",
          warehouse.getDatabasePath(db2).toString(), db2.getLocationUri());

      List<String> dbs = client.getDatabases(".*");

      assertTrue("first database is not " + TEST_DB1_NAME, dbs.contains(TEST_DB1_NAME));
      assertTrue("second database is not " + TEST_DB2_NAME, dbs.contains(TEST_DB2_NAME));

      client.dropDatabase(TEST_DB1_NAME);
      client.dropDatabase(TEST_DB2_NAME);
      silentDropDatabase(TEST_DB1_NAME);
      silentDropDatabase(TEST_DB2_NAME);
    } catch (Throwable e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testDatabase() failed.");
      throw e;
    }
  }

  @Test
  public void testDatabaseLocationWithPermissionProblems() throws Exception {

    // Note: The following test will fail if you are running this test as root. Setting
    // permission to '0' on the database folder will not preclude root from being able
    // to create the necessary files.

    if (System.getProperty("user.name").equals("root")) {
      System.err.println("Skipping test because you are running as root!");
      return;
    }

    silentDropDatabase(TEST_DB1_NAME);

    String dbLocation =
        MetastoreConf.getVar(conf, ConfVars.WAREHOUSE) + "/test/_testDB_create_";
    FileSystem fs = FileSystem.get(new Path(dbLocation).toUri(), conf);
    fs.mkdirs(
        new Path(MetastoreConf.getVar(conf, ConfVars.WAREHOUSE) + "/test"),
        new FsPermission((short) 0));
    Database db = new DatabaseBuilder()
        .setName(TEST_DB1_NAME)
        .setManagedLocation(dbLocation)
        .build(conf);


    boolean createFailed = false;
    try {
      client.createDatabase(db);
    } catch (MetaException cantCreateDB) {
      createFailed = true;
    } finally {
      // Cleanup
      if (!createFailed) {
        try {
          client.dropDatabase(TEST_DB1_NAME);
        } catch(Exception e) {
          System.err.println("Failed to remove database in cleanup: " + e.getMessage());
        }
      }

      fs.setPermission(new Path(MetastoreConf.getVar(conf, ConfVars.WAREHOUSE) + "/test"),
          new FsPermission((short) 755));
      fs.delete(new Path(MetastoreConf.getVar(conf, ConfVars.WAREHOUSE) + "/test"), true);
    }

    assertTrue("Database creation succeeded even with permission problem", createFailed);
  }

  @Test
  public void testDatabaseLocation() throws Throwable {
    try {
      // clear up any existing databases
      silentDropDatabase(TEST_DB1_NAME);

      String dbLocation =
          MetastoreConf.getVar(conf, ConfVars.WAREHOUSE) + "/_testDB_create_";
      new DatabaseBuilder()
          .setName(TEST_DB1_NAME)
          .setLocation(dbLocation)
          .create(client, conf);

      Database db = client.getDatabase(TEST_DB1_NAME);

      assertEquals("name of returned db is different from that of inserted db",
          TEST_DB1_NAME, db.getName());
      assertEquals("location of the returned db is different from that of inserted db",
          warehouse.getDnsPath(new Path(dbLocation)).toString(), db.getLocationUri());

      client.dropDatabase(TEST_DB1_NAME);
      silentDropDatabase(TEST_DB1_NAME);

      boolean objectNotExist = false;
      try {
        client.getDatabase(TEST_DB1_NAME);
      } catch (NoSuchObjectException e) {
        objectNotExist = true;
      }
      assertTrue("Database " + TEST_DB1_NAME + " exists ", objectNotExist);

      dbLocation =
          MetastoreConf.getVar(conf, ConfVars.WAREHOUSE) + "/_testDB_file_";
      FileSystem fs = FileSystem.get(new Path(dbLocation).toUri(), conf);
      fs.createNewFile(new Path(dbLocation));
      fs.deleteOnExit(new Path(dbLocation));
      db = new DatabaseBuilder()
          .setName(TEST_DB1_NAME)
          .setLocation(dbLocation)
          .build(conf);

      boolean createFailed = false;
      try {
        client.createDatabase(db);
      } catch (MetaException cantCreateDB) {
        System.err.println(cantCreateDB.getMessage());
        createFailed = true;
      }
      assertTrue("Database creation succeeded even location exists and is a file", createFailed);

      objectNotExist = false;
      try {
        client.getDatabase(TEST_DB1_NAME);
      } catch (NoSuchObjectException e) {
        objectNotExist = true;
      }
      assertTrue("Database " + TEST_DB1_NAME + " exists when location is specified and is a file",
          objectNotExist);

    } catch (Throwable e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testDatabaseLocation() failed.");
      throw e;
    }
  }


  @Test
  public void testDatabaseLocationOnDrop() throws Throwable {
    try {
      // clear up any existing databases
      silentDropDatabase(TEST_DB1_NAME);

      String dbLocation =
          MetastoreConf.getVar(conf, ConfVars.WAREHOUSE_EXTERNAL) + "/testdb1.db";
      String mgdLocation =
          MetastoreConf.getVar(conf, ConfVars.WAREHOUSE) + "/testdb1.db";
      new DatabaseBuilder()
          .setName(TEST_DB1_NAME)
          .setLocation(dbLocation)
          .setManagedLocation(mgdLocation)
          .create(client, conf);

      Database db = client.getDatabase(TEST_DB1_NAME);

      assertEquals("name of returned db is different from that of inserted db",
          TEST_DB1_NAME, db.getName());
      assertEquals("location of the returned db is different from that of inserted db",
          warehouse.getDnsPath(new Path(dbLocation)).toString(), db.getLocationUri());
      assertEquals("managed location of the returned db is different from that of inserted db",
          warehouse.getDnsPath(new Path(mgdLocation)).toString(), db.getManagedLocationUri());

      client.dropDatabase(TEST_DB1_NAME, true, false, true);

      boolean objectNotExist = false;
      try {
        client.getDatabase(TEST_DB1_NAME);
      } catch (NoSuchObjectException e) {
        objectNotExist = true;
      }
      assertTrue("Database " + TEST_DB1_NAME + " exists ", objectNotExist);

      FileSystem fs = FileSystem.get(new Path(dbLocation).toUri(), conf);
      assertFalse("Database's location not deleted", fs.exists(new Path(dbLocation)));
      fs = FileSystem.get(new Path(mgdLocation).toUri(), conf);
      assertFalse("Database's managed location not deleted", fs.exists(new Path(mgdLocation)));
    } catch (Throwable e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testDatabaseLocationOnDrop() failed.");
      throw e;
    }
  }

  @Test
  public void testSimpleTypeApi() throws Exception {
    try {
      client.dropType(ColumnType.INT_TYPE_NAME);

      Type typ1 = new Type();
      typ1.setName(ColumnType.INT_TYPE_NAME);
      boolean ret = client.createType(typ1);
      assertTrue("Unable to create type", ret);

      Type typ1_2 = client.getType(ColumnType.INT_TYPE_NAME);
      assertNotNull(typ1_2);
      assertEquals(typ1.getName(), typ1_2.getName());

      ret = client.dropType(ColumnType.INT_TYPE_NAME);
      assertTrue("unable to drop type integer", ret);

      boolean exceptionThrown = false;
      try {
        client.getType(ColumnType.INT_TYPE_NAME);
      } catch (NoSuchObjectException e) {
        exceptionThrown = true;
      }
      assertTrue("Expected NoSuchObjectException", exceptionThrown);
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testSimpleTypeApi() failed.");
      throw e;
    }
  }

  // TODO:pc need to enhance this with complex fields and getType_all function
  @Test
  public void testComplexTypeApi() throws Exception {
    try {
      client.dropType("Person");

      Type typ1 = new Type();
      typ1.setName("Person");
      typ1.setFields(new ArrayList<>(2));
      typ1.getFields().add(
          new FieldSchema("name", ColumnType.STRING_TYPE_NAME, ""));
      typ1.getFields().add(
          new FieldSchema("income", ColumnType.INT_TYPE_NAME, ""));
      boolean ret = client.createType(typ1);
      assertTrue("Unable to create type", ret);

      Type typ1_2 = client.getType("Person");
      assertNotNull("type Person not found", typ1_2);
      assertEquals(typ1.getName(), typ1_2.getName());
      assertEquals(typ1.getFields().size(), typ1_2.getFields().size());
      assertEquals(typ1.getFields().get(0), typ1_2.getFields().get(0));
      assertEquals(typ1.getFields().get(1), typ1_2.getFields().get(1));

      client.dropType("Family");

      Type fam = new Type();
      fam.setName("Family");
      fam.setFields(new ArrayList<>(2));
      fam.getFields().add(
          new FieldSchema("name", ColumnType.STRING_TYPE_NAME, ""));
      fam.getFields().add(
          new FieldSchema("members",
              ColumnType.getListType(typ1.getName()), ""));

      ret = client.createType(fam);
      assertTrue("Unable to create type " + fam.getName(), ret);

      Type fam2 = client.getType("Family");
      assertNotNull("type Person not found", fam2);
      assertEquals(fam.getName(), fam2.getName());
      assertEquals(fam.getFields().size(), fam2.getFields().size());
      assertEquals(fam.getFields().get(0), fam2.getFields().get(0));
      assertEquals(fam.getFields().get(1), fam2.getFields().get(1));

      ret = client.dropType("Family");
      assertTrue("unable to drop type Family", ret);

      ret = client.dropType("Person");
      assertTrue("unable to drop type Person", ret);

      boolean exceptionThrown = false;
      try {
        client.getType("Person");
      } catch (NoSuchObjectException e) {
        exceptionThrown = true;
      }
      assertTrue("Expected NoSuchObjectException", exceptionThrown);
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testComplexTypeApi() failed.");
      throw e;
    }
  }

  @Test
  public void testSimpleTable() throws Exception {
    try {
      String dbName = "simpdb";
      String tblName = "simptbl";
      String tblName2 = "simptbl2";
      String typeName = "Person";

      client.dropTable(dbName, tblName);
      silentDropDatabase(dbName);

      new DatabaseBuilder()
          .setName(dbName)
          .create(client, conf);

      client.dropType(typeName);
      Type typ1 = new Type();
      typ1.setName(typeName);
      typ1.setFields(new ArrayList<>(2));
      typ1.getFields().add(
          new FieldSchema("name", ColumnType.STRING_TYPE_NAME, ""));
      typ1.getFields().add(
          new FieldSchema("income", ColumnType.INT_TYPE_NAME, ""));
      client.createType(typ1);

      Table tbl = new TableBuilder()
          .setDbName(dbName)
          .setTableName(tblName)
          .setCols(typ1.getFields())
          .setNumBuckets(1)
          .addBucketCol("name")
          .addStorageDescriptorParam("test_param_1", "Use this for comments etc")
          .create(client, conf);

      if (isThriftClient) {
        // the createTable() above does not update the location in the 'tbl'
        // object when the client is a thrift client and the code below relies
        // on the location being present in the 'tbl' object - so get the table
        // from the metastore
        tbl = client.getTable(dbName, tblName);
      }

      Table tbl2 = client.getTable(dbName, tblName);
      assertNotNull(tbl2);
      Assert.assertTrue(tbl2.isSetId());
      assertEquals(tbl2.getDbName(), dbName);
      assertEquals(tbl2.getTableName(), tblName);
      assertEquals(tbl2.getSd().getCols().size(), typ1.getFields().size());
      assertEquals(tbl2.getSd().isCompressed(), false);
      assertEquals(tbl2.getSd().getNumBuckets(), 1);
      assertEquals(tbl2.getSd().getLocation(), tbl.getSd().getLocation());
      assertNotNull(tbl2.getSd().getSerdeInfo());
      tbl.getSd().getSerdeInfo().setParameters(new HashMap<>());
      tbl.getSd().getSerdeInfo().getParameters().put(ColumnType.SERIALIZATION_FORMAT, "1");

      tbl2.setTableName(tblName2);
      tbl2.setParameters(new HashMap<>());
      tbl2.getParameters().put("EXTERNAL", "TRUE");
      tbl2.getSd().setLocation(tbl.getSd().getLocation() + "-2");

      List<FieldSchema> fieldSchemas = client.getFields(dbName, tblName);
      assertNotNull(fieldSchemas);
      assertEquals(fieldSchemas.size(), tbl.getSd().getCols().size());
      for (FieldSchema fs : tbl.getSd().getCols()) {
        assertTrue("fieldSchemas variable doesn't contain " + fs, fieldSchemas.contains(fs));
      }

      List<FieldSchema> fieldSchemasFull = client.getSchema(dbName, tblName);
      assertNotNull(fieldSchemasFull);
      assertEquals(fieldSchemasFull.size(), tbl.getSd().getCols().size()
          + tbl.getPartitionKeys().size());
      for (FieldSchema fs : tbl.getSd().getCols()) {
        assertTrue("fieldSchemasFull variable doesn't contain " + fs, fieldSchemasFull.contains(fs));

      }
      for (FieldSchema fs : tbl.getPartitionKeys()) {
        assertTrue("fieldSchemasFull variable doesn't contain " + fs, fieldSchemasFull.contains(fs));
      }

      tbl2.unsetId();
      client.createTable(tbl2);
      if (isThriftClient) {
        tbl2 = client.getTable(tbl2.getDbName(), tbl2.getTableName());
      }

      Table tbl3 = client.getTable(dbName, tblName2);
      assertNotNull(tbl3);
      assertEquals(tbl3.getDbName(), dbName);
      assertEquals(tbl3.getTableName(), tblName2);
      assertEquals(tbl3.getSd().getCols().size(), typ1.getFields().size());
      assertEquals(tbl3.getSd().isCompressed(), false);
      assertEquals(tbl3.getSd().getNumBuckets(), 1);
      assertEquals(tbl3.getSd().getLocation(), tbl2.getSd().getLocation());
      assertEquals(tbl3.getParameters(), tbl2.getParameters());

      fieldSchemas = client.getFields(dbName, tblName2);
      assertNotNull(fieldSchemas);
      assertEquals(fieldSchemas.size(), tbl2.getSd().getCols().size());
      for (FieldSchema fs : tbl2.getSd().getCols()) {
        assertTrue("fieldSchemas variable doesn't contain " + fs, fieldSchemas.contains(fs));
      }

      fieldSchemasFull = client.getSchema(dbName, tblName2);
      assertNotNull(fieldSchemasFull);
      assertEquals(fieldSchemasFull.size(), tbl2.getSd().getCols().size()
          + tbl2.getPartitionKeys().size());
      for (FieldSchema fs : tbl2.getSd().getCols()) {
        assertTrue("fieldSchemasFull variable doesn't contain " + fs, fieldSchemasFull.contains(fs));
      }
      for (FieldSchema fs : tbl2.getPartitionKeys()) {
        assertTrue("fieldSchemasFull variable doesn't contain " + fs, fieldSchemasFull.contains(fs));
      }

      assertEquals("Use this for comments etc", tbl2.getSd().getParameters()
          .get("test_param_1"));
      assertEquals("name", tbl2.getSd().getBucketCols().get(0));
      assertTrue("Partition key list is not empty",
          (tbl2.getPartitionKeys() == null)
              || (tbl2.getPartitionKeys().size() == 0));

      //test get_table_objects_by_name functionality
      ArrayList<String> tableNames = new ArrayList<>();
      tableNames.add(tblName2);
      tableNames.add(tblName);
      tableNames.add(tblName2);
      List<Table> foundTables = client.getTableObjectsByName(dbName, tableNames);

      assertEquals(2, foundTables.size());
      for (Table t: foundTables) {
        if (t.getTableName().equals(tblName2)) {
          assertEquals(t.getSd().getLocation(), tbl2.getSd().getLocation());
        } else {
          assertEquals(t.getTableName(), tblName);
          assertEquals(t.getSd().getLocation(), tbl.getSd().getLocation());
        }
        assertEquals(t.getSd().getCols().size(), typ1.getFields().size());
        assertEquals(t.getSd().isCompressed(), false);
        assertEquals(foundTables.get(0).getSd().getNumBuckets(), 1);
        assertNotNull(t.getSd().getSerdeInfo());
        assertEquals(t.getDbName(), dbName);
      }

      tableNames.add(1, "table_that_doesnt_exist");
      foundTables = client.getTableObjectsByName(dbName, tableNames);
      assertEquals(foundTables.size(), 2);

      InvalidOperationException ioe = null;
      try {
        foundTables = client.getTableObjectsByName(dbName, null);
      } catch (InvalidOperationException e) {
        ioe = e;
      }
      assertNotNull(ioe);
      assertTrue("Table not found", ioe.getMessage().contains("null tables"));

      UnknownDBException udbe = null;
      try {
        foundTables = client.getTableObjectsByName("db_that_doesnt_exist", tableNames);
      } catch (UnknownDBException e) {
        udbe = e;
      }
      assertNotNull(udbe);
      assertTrue("DB not found",
          udbe.getMessage().contains("not find database hive.db_that_doesnt_exist"));

      udbe = null;
      try {
        foundTables = client.getTableObjectsByName("", tableNames);
      } catch (UnknownDBException e) {
        udbe = e;
      }
      assertNotNull(udbe);
      assertTrue("DB not found", udbe.getMessage().contains("is null or empty"));

      FileSystem fs = FileSystem.get((new Path(tbl.getSd().getLocation())).toUri(), conf);
      client.dropTable(dbName, tblName);
      Path pathTbl = new Path(tbl.getSd().getLocation());
      assertFalse(pathTbl + " exists", fs.exists(pathTbl));

      client.dropTable(dbName, tblName2);
      Path pathTbl2 = new Path(tbl2.getSd().getLocation());
      assertTrue(pathTbl2 + " doesn't exist", fs.exists(pathTbl2));

      client.dropType(typeName);
      client.dropDatabase(dbName);
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testSimpleTable() failed.");
      throw e;
    }
  }

  // Tests that in the absence of stats for partitions, and/or absence of columns
  // to get stats for, the metastore does not break. See HIVE-12083 for motivation.
  @Test
  public void testStatsFastTrivial() throws Throwable {
    String dbName = "tstatsfast";
    String tblName = "t1";
    String tblOwner = "statstester";
    String typeName = "Person";
    int lastAccessed = 12083;

    cleanUp(dbName,tblName,typeName);

    List<List<String>> values = new ArrayList<>();
    values.add(makeVals("2008-07-01 14:13:12", "14"));
    values.add(makeVals("2008-07-01 14:13:12", "15"));
    values.add(makeVals("2008-07-02 14:13:12", "15"));
    values.add(makeVals("2008-07-03 14:13:12", "151"));

    createMultiPartitionTableSchema(dbName, tblName, typeName, values);

    List<String> emptyColNames = new ArrayList<>();
    List<String> emptyPartNames = new ArrayList<>();

    List<String> colNames = new ArrayList<>();
    colNames.add("name");
    colNames.add("income");
    List<String> partNames = client.listPartitionNames(dbName,tblName,(short)-1);

    assertEquals(0,emptyColNames.size());
    assertEquals(0,emptyPartNames.size());
    assertEquals(2,colNames.size());
    assertEquals(4,partNames.size());

    // Test for both colNames and partNames being empty:
    AggrStats aggrStatsEmpty = client.getAggrColStatsFor(dbName,tblName,emptyColNames,emptyPartNames, ENGINE);
    assertNotNull(aggrStatsEmpty); // short-circuited on client-side, verifying that it's an empty object, not null
    assertEquals(0,aggrStatsEmpty.getPartsFound());
    assertNotNull(aggrStatsEmpty.getColStats());
    assert(aggrStatsEmpty.getColStats().isEmpty());

    // Test for only colNames being empty
    AggrStats aggrStatsOnlyParts = client.getAggrColStatsFor(dbName,tblName,emptyColNames,partNames, ENGINE);
    assertNotNull(aggrStatsOnlyParts); // short-circuited on client-side, verifying that it's an empty object, not null
    assertEquals(0,aggrStatsOnlyParts.getPartsFound());
    assertNotNull(aggrStatsOnlyParts.getColStats());
    assert(aggrStatsOnlyParts.getColStats().isEmpty());

    // Test for only partNames being empty
    AggrStats aggrStatsOnlyCols = client.getAggrColStatsFor(dbName,tblName,colNames,emptyPartNames, ENGINE);
    assertNotNull(aggrStatsOnlyCols); // short-circuited on client-side, verifying that it's an empty object, not null
    assertEquals(0,aggrStatsOnlyCols.getPartsFound());
    assertNotNull(aggrStatsOnlyCols.getColStats());
    assert(aggrStatsOnlyCols.getColStats().isEmpty());

    // Test for valid values for both.
    AggrStats aggrStatsFull = client.getAggrColStatsFor(dbName,tblName,colNames,partNames, ENGINE);
    assertNotNull(aggrStatsFull);
    assertEquals(0,aggrStatsFull.getPartsFound()); // would still be empty, because no stats are actually populated.
    assertNotNull(aggrStatsFull.getColStats());
    assert(aggrStatsFull.getColStats().isEmpty());

  }

  @Test
  public void testColumnStatistics() throws Throwable {

    String dbName = "columnstatstestdb";
    String tblName = "tbl";
    String typeName = "Person";
    String tblOwner = "testowner";
    int lastAccessed = 6796;

    try {
      cleanUp(dbName, tblName, typeName);
      new DatabaseBuilder()
          .setName(dbName)
          .create(client, conf);
      createTableForTestFilter(dbName, tblName, tblOwner, lastAccessed, true);

      // Create a ColumnStatistics Obj
      String[] colName = new String[]{"income", "name"};
      double lowValue = 50000.21;
      double highValue = 1200000.4525;
      long numNulls = 3;
      long numDVs = 22;
      double avgColLen = 50.30;
      long maxColLen = 102;
      String[] colType = new String[] {"double", "string"};
      boolean isTblLevel = true;
      String partName = null;
      List<ColumnStatisticsObj> statsObjs = new ArrayList<>();

      ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc();
      statsDesc.setDbName(dbName);
      statsDesc.setTableName(tblName);
      statsDesc.setIsTblLevel(isTblLevel);
      statsDesc.setPartName(partName);

      ColumnStatisticsObj statsObj = new ColumnStatisticsObj();
      statsObj.setColName(colName[0]);
      statsObj.setColType(colType[0]);

      ColumnStatisticsData statsData = new ColumnStatisticsData();
      DoubleColumnStatsData numericStats = new DoubleColumnStatsData();
      statsData.setDoubleStats(numericStats);

      statsData.getDoubleStats().setHighValue(highValue);
      statsData.getDoubleStats().setLowValue(lowValue);
      statsData.getDoubleStats().setNumDVs(numDVs);
      statsData.getDoubleStats().setNumNulls(numNulls);

      statsObj.setStatsData(statsData);
      statsObjs.add(statsObj);

      statsObj = new ColumnStatisticsObj();
      statsObj.setColName(colName[1]);
      statsObj.setColType(colType[1]);

      statsData = new ColumnStatisticsData();
      StringColumnStatsData stringStats = new StringColumnStatsData();
      statsData.setStringStats(stringStats);
      statsData.getStringStats().setAvgColLen(avgColLen);
      statsData.getStringStats().setMaxColLen(maxColLen);
      statsData.getStringStats().setNumDVs(numDVs);
      statsData.getStringStats().setNumNulls(numNulls);

      statsObj.setStatsData(statsData);
      statsObjs.add(statsObj);

      ColumnStatistics colStats = new ColumnStatistics();
      colStats.setStatsDesc(statsDesc);
      colStats.setStatsObj(statsObjs);
      colStats.setEngine(ENGINE);

      // write stats objs persistently
      client.updateTableColumnStatistics(colStats);

      // retrieve the stats obj that was just written
      ColumnStatisticsObj colStats2 = client.getTableColumnStatistics(
          dbName, tblName, Lists.newArrayList(colName[0]), ENGINE).get(0);

     // compare stats obj to ensure what we get is what we wrote
      assertNotNull(colStats2);
      assertEquals(colStats2.getColName(), colName[0]);
      assertEquals(colStats2.getStatsData().getDoubleStats().getLowValue(), lowValue, 0.01);
      assertEquals(colStats2.getStatsData().getDoubleStats().getHighValue(), highValue, 0.01);
      assertEquals(colStats2.getStatsData().getDoubleStats().getNumNulls(), numNulls);
      assertEquals(colStats2.getStatsData().getDoubleStats().getNumDVs(), numDVs);

      // test delete column stats; if no col name is passed all column stats associated with the
      // table is deleted
      boolean status = client.deleteTableColumnStatistics(dbName, tblName, null, ENGINE);
      assertTrue(status);
      // try to query stats for a column for which stats doesn't exist
      List<ColumnStatisticsObj> stats = client.getTableColumnStatistics(
          dbName, tblName, Lists.newArrayList(colName[1]), ENGINE);
      assertTrue("stats are not empty: " + stats, stats.isEmpty());

      colStats.setStatsDesc(statsDesc);
      colStats.setStatsObj(statsObjs);

      // update table level column stats
      client.updateTableColumnStatistics(colStats);

      // query column stats for column whose stats were updated in the previous call
      List<ColumnStatisticsObj> colStats3 = client.getTableColumnStatistics(
          dbName, tblName, Lists.newArrayList(colName), ENGINE);
      assertEquals(2, colStats3.size());
      DeleteColumnStatisticsRequest request = new DeleteColumnStatisticsRequest(dbName, tblName);
      request.setTableLevel(true);
      request.setEngine(ENGINE);
      // multiple columns
      request.setCol_names(Arrays.asList(colName));
      assertTrue(client.deleteColumnStatistics(request));
      colStats3 = client.getTableColumnStatistics(
          dbName, tblName, Lists.newArrayList(colName), ENGINE);
      assertTrue("stats are not empty: " + colStats3, colStats3.isEmpty());

      // partition level column statistics test
      // create a table with multiple partitions
      cleanUp(dbName, tblName, typeName);

      List<List<String>> values = new ArrayList<>();
      values.add(makeVals("2008-07-01 14:13:12", "14"));
      values.add(makeVals("2008-07-01 14:13:12", "15"));
      values.add(makeVals("2008-07-02 14:13:12", "15"));
      values.add(makeVals("2008-07-03 14:13:12", "151"));

      createMultiPartitionTableSchema(dbName, tblName, typeName, values);

      List<String> partitions = client.listPartitionNames(dbName, tblName, (short)-1);

      partName = partitions.get(0);
      isTblLevel = false;

      // create a new columnstatistics desc to represent partition level column stats
      statsDesc = new ColumnStatisticsDesc();
      statsDesc.setDbName(dbName);
      statsDesc.setTableName(tblName);
      statsDesc.setPartName(partName);
      statsDesc.setIsTblLevel(isTblLevel);

      colStats = new ColumnStatistics();
      colStats.setStatsDesc(statsDesc);
      colStats.setStatsObj(statsObjs);
      colStats.setEngine(ENGINE);

     client.updatePartitionColumnStatistics(colStats);

     colStats2 = client.getPartitionColumnStatistics(dbName, tblName,
         Lists.newArrayList(partName), Lists.newArrayList(colName[1]), ENGINE).get(partName).get(0);

     // compare stats obj to ensure what we get is what we wrote
     assertNotNull(colStats2);
     assertEquals(colStats.getStatsDesc().getPartName(), partName);
     assertEquals(colStats2.getColName(), colName[1]);
     assertEquals(colStats2.getStatsData().getStringStats().getMaxColLen(), maxColLen);
     assertEquals(colStats2.getStatsData().getStringStats().getAvgColLen(), avgColLen, 0.01);
     assertEquals(colStats2.getStatsData().getStringStats().getNumNulls(), numNulls);
     assertEquals(colStats2.getStatsData().getStringStats().getNumDVs(), numDVs);

     // test stats deletion at partition level
     client.deletePartitionColumnStatistics(dbName, tblName, partName, colName[1], ENGINE);

     colStats2 = client.getPartitionColumnStatistics(dbName, tblName,
         Lists.newArrayList(partName), Lists.newArrayList(colName[0]), ENGINE).get(partName).get(0);

     // test get stats on a column for which stats doesn't exist
     Map<String, List<ColumnStatisticsObj>> stats2 = client.getPartitionColumnStatistics(dbName, tblName,
         Lists.newArrayList(partName), Lists.newArrayList(colName[1]), ENGINE);
     assertTrue("stats are not empty: " + stats2, stats2.isEmpty());

     request.setTableLevel(false);
     request.setCol_names(Arrays.asList(colName));
     request.addToPart_names(partName);
     // no column name set
     request.unsetCol_names();
     assertTrue(client.deleteColumnStatistics(request));
     stats2 = client.getPartitionColumnStatistics(dbName, tblName,
          Lists.newArrayList(partName), Lists.newArrayList(colName), ENGINE);
     assertTrue("stats are not empty: " + stats2, stats2.isEmpty());

     client.updatePartitionColumnStatistics(colStats);
     ColumnStatistics colStats1 = new ColumnStatistics(colStats);
     colStats1.getStatsDesc().setPartName(partitions.get(1));
     client.updatePartitionColumnStatistics(colStats1);
     colStats1 = new ColumnStatistics(colStats);
     colStats1.getStatsDesc().setPartName(partitions.get(2));
     client.updatePartitionColumnStatistics(colStats1);

     request.unsetPart_names();
     request.setPart_names(Arrays.asList(partitions.get(0), partitions.get(1)));
     request.setCol_names(Arrays.asList(colName[0]));
     assertTrue(client.deleteColumnStatistics(request));
     stats2 = client.getPartitionColumnStatistics(dbName, tblName,
         Lists.newArrayList(partitions.get(0), partitions.get(1), partitions.get(2)), Lists.newArrayList(colName), ENGINE);
     assertEquals(1, stats2.get(partitions.get(0)).size());
     assertEquals(colName[1], stats2.get(partitions.get(0)).get(0).getColName());
     assertEquals(1, stats2.get(partitions.get(1)).size());
     assertEquals(colName[1], stats2.get(partitions.get(1)).get(0).getColName());
     assertEquals(2, stats2.get(partitions.get(2)).size());

     // no column
     request.unsetCol_names();
     assertTrue(client.deleteColumnStatistics(request));
     stats2 = client.getPartitionColumnStatistics(dbName, tblName,
          Lists.newArrayList(partitions.get(0), partitions.get(1), partitions.get(2)), Lists.newArrayList(colName), ENGINE);
     assertEquals(1, stats2.size());
     assertEquals(2, stats2.get(partitions.get(2)).size());

     // no partition or column name is set
     request.unsetPart_names();
     client.updatePartitionColumnStatistics(colStats);
     assertTrue(client.deleteColumnStatistics(request));
     stats2 = client.getPartitionColumnStatistics(dbName, tblName,
          Lists.newArrayList(partitions), Lists.newArrayList(colName), ENGINE);
     assertTrue("stats are not empty: " + stats2, stats2.isEmpty());
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testColumnStatistics() failed.");
      throw e;
    } finally {
      cleanUp(dbName, tblName, typeName);
    }
  }

  @Test(expected = MetaException.class)
  public void testGetSchemaWithNoClassDefFoundError() throws TException {
    String dbName = "testDb";
    String tblName = "testTable";

    client.dropTable(dbName, tblName);
    silentDropDatabase(dbName);

    new DatabaseBuilder()
        .setName(dbName)
        .create(client, conf);

    Table tbl = new TableBuilder()
        .setDbName(dbName)
        .setTableName(tblName)
        .addCol("name", ColumnType.STRING_TYPE_NAME, "")
        .setSerdeLib("no.such.class")
        .create(client, conf);

    client.getSchema(dbName, tblName);
  }

  @Test
  public void testCreateAndGetTableWithDriver() throws Exception {
    String dbName = "createDb";
    String tblName = "createTbl";

    client.dropTable(dbName, tblName);
    silentDropDatabase(dbName);
    new DatabaseBuilder()
        .setName(dbName)
        .create(client, conf);

    createTable(dbName, tblName);
    Table tblRead = client.getTable(dbName, tblName);
    Assert.assertTrue(tblRead.isSetId());
    long firstTableId = tblRead.getId();

    createTable(dbName, tblName + "_2");
    Table tblRead2 = client.getTable(dbName, tblName + "_2");
    Assert.assertTrue(tblRead2.isSetId());
    Assert.assertNotEquals(firstTableId, tblRead2.getId());
  }

  @Test
  public void testCreateTableSettingId() throws Exception {
    String dbName = "createDb";
    String tblName = "createTbl";

    client.dropTable(dbName, tblName);
    silentDropDatabase(dbName);
    new DatabaseBuilder()
        .setName(dbName)
        .create(client, conf);

    Table table = new TableBuilder()
        .setDbName(dbName)
        .setTableName(tblName)
        .addCol("foo", "string")
        .addCol("bar", "string")
        .build(conf);
    table.setId(1);
    client.createTable(table);
  }

  @Test
  public void testAlterTable() throws Exception {
    String dbName = "alterdb";
    String invTblName = "alter§tbl";
    String tblName = "altertbl";

    try {
      client.dropTable(dbName, tblName);
      silentDropDatabase(dbName);

      String dbLocation =
          MetastoreConf.getVar(conf, ConfVars.WAREHOUSE_EXTERNAL) + "/_testDB_table_create_";
      String mgdLocation =
          MetastoreConf.getVar(conf, ConfVars.WAREHOUSE) + "/_testDB_table_create_";
      new DatabaseBuilder()
          .setName(dbName)
          .setLocation(dbLocation)
          .setManagedLocation(mgdLocation)
          .create(client, conf);

      ArrayList<FieldSchema> invCols = new ArrayList<>(2);
      invCols.add(new FieldSchema("n-ame", ColumnType.STRING_TYPE_NAME, ""));
      invCols.add(new FieldSchema("in.come", ColumnType.INT_TYPE_NAME, ""));

      Table tbl = new TableBuilder()
          .setDbName(dbName)
          .setTableName(invTblName)
          .setCols(invCols)
          .build(conf);

      boolean failed = false;
      try {
        client.createTable(tbl);
      } catch (InvalidObjectException ex) {
        failed = true;
      }
      if (!failed) {
        assertTrue("Able to create table with invalid name: " + invTblName,
            false);
      }

      // create an invalid table which has wrong column type
      ArrayList<FieldSchema> invColsInvType = new ArrayList<>(2);
      invColsInvType.add(new FieldSchema("name", ColumnType.STRING_TYPE_NAME, ""));
      invColsInvType.add(new FieldSchema("income", "xyz", ""));
      tbl.setTableName(tblName);
      tbl.getSd().setCols(invColsInvType);
      boolean failChecker = false;
      try {
        client.createTable(tbl);
      } catch (InvalidObjectException ex) {
        failChecker = true;
      }
      if (!failChecker) {
        assertTrue("Able to create table with invalid column type: " + invTblName,
            false);
      }

      ArrayList<FieldSchema> cols = new ArrayList<>(2);
      cols.add(new FieldSchema("name", ColumnType.STRING_TYPE_NAME, ""));
      cols.add(new FieldSchema("income", ColumnType.INT_TYPE_NAME, ""));

      // create a valid table
      tbl.setTableName(tblName);
      tbl.getSd().setCols(cols);
      client.createTable(tbl);

      if (isThriftClient) {
        tbl = client.getTable(tbl.getDbName(), tbl.getTableName());
      }

      // now try to invalid alter table
      Table tbl2 = client.getTable(dbName, tblName);
      failed = false;
      try {
        tbl2.setTableName(invTblName);
        tbl2.getSd().setCols(invCols);
        client.alter_table(dbName, tblName, tbl2);
      } catch (InvalidOperationException ex) {
        failed = true;
      }
      if (!failed) {
        assertTrue("Able to rename table with invalid name: " + invTblName,
            false);
      }

      //try an invalid alter table with partition key name
      Table tbl_pk = client.getTable(tbl.getDbName(), tbl.getTableName());
      List<FieldSchema> partitionKeys = tbl_pk.getPartitionKeys();
      for (FieldSchema fs : partitionKeys) {
        fs.setName("invalid_to_change_name");
        fs.setComment("can_change_comment");
      }
      tbl_pk.setPartitionKeys(partitionKeys);
      try {
        client.alter_table(dbName, tblName, tbl_pk);
      } catch (InvalidOperationException ex) {
        failed = true;
      }
      assertTrue("Should not have succeeded in altering partition key name", failed);

      //try a valid alter table partition key comment
      failed = false;
      tbl_pk = client.getTable(tbl.getDbName(), tbl.getTableName());
      partitionKeys = tbl_pk.getPartitionKeys();
      for (FieldSchema fs : partitionKeys) {
        fs.setComment("can_change_comment");
      }
      tbl_pk.setPartitionKeys(partitionKeys);
      try {
        client.alter_table(dbName, tblName, tbl_pk);
      } catch (InvalidOperationException ex) {
        failed = true;
      }
      assertFalse("Should not have failed alter table partition comment", failed);
      Table newT = client.getTable(tbl.getDbName(), tbl.getTableName());
      assertEquals(partitionKeys, newT.getPartitionKeys());

      // try a valid alter table
      tbl2.setTableName(tblName + "_renamed");
      tbl2.getSd().setCols(cols);
      tbl2.getSd().setNumBuckets(32);
      client.alter_table(dbName, tblName, tbl2);
      Table tbl3 = client.getTable(dbName, tbl2.getTableName());
      assertEquals("Alter table didn't succeed. Num buckets is different ",
          tbl2.getSd().getNumBuckets(), tbl3.getSd().getNumBuckets());
      // check that data has moved
      FileSystem fs = FileSystem.get((new Path(tbl.getSd().getLocation())).toUri(), conf);
      assertFalse("old table location still exists", fs.exists(new Path(tbl
          .getSd().getLocation())));
      assertTrue("data did not move to new location", fs.exists(new Path(tbl3
          .getSd().getLocation())));

      if (!isThriftClient) {
        assertEquals("alter table didn't move data correct location", tbl3
            .getSd().getLocation(), tbl2.getSd().getLocation());
      }

      // alter table with invalid column type
      tbl_pk.getSd().setCols(invColsInvType);
      failed = false;
      try {
        client.alter_table(dbName, tbl2.getTableName(), tbl_pk);
      } catch (InvalidOperationException ex) {
        failed = true;
      }
      assertTrue("Should not have succeeded in altering column", failed);

    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testSimpleTable() failed.");
      throw e;
    } finally {
      silentDropDatabase(dbName);
    }
  }

  @Test
  public void testComplexTable() throws Exception {

    String dbName = "compdb";
    String tblName = "comptbl";
    String typeName = "Person";

    try {
      client.dropTable(dbName, tblName);
      silentDropDatabase(dbName);
      new DatabaseBuilder()
          .setName(dbName)
          .create(client, conf);

      client.dropType(typeName);
      Type typ1 = new Type();
      typ1.setName(typeName);
      typ1.setFields(new ArrayList<>(2));
      typ1.getFields().add(
          new FieldSchema("name", ColumnType.STRING_TYPE_NAME, ""));
      typ1.getFields().add(
          new FieldSchema("income", ColumnType.INT_TYPE_NAME, ""));
      client.createType(typ1);

      Table tbl = new TableBuilder()
          .setDbName(dbName)
          .setTableName(tblName)
          .setCols(typ1.getFields())
          .addPartCol("ds", ColumnType.DATE_TYPE_NAME)
          .addPartCol("hr", ColumnType.INT_TYPE_NAME)
          .setNumBuckets(1)
          .addBucketCol("name")
          .addStorageDescriptorParam("test_param_1","Use this for comments etc")
          .create(client, conf);

      Table tbl2 = client.getTable(dbName, tblName);
      assertEquals(tbl2.getDbName(), dbName);
      assertEquals(tbl2.getTableName(), tblName);
      assertEquals(tbl2.getSd().getCols().size(), typ1.getFields().size());
      assertFalse(tbl2.getSd().isCompressed());
      assertFalse(tbl2.getSd().isStoredAsSubDirectories());
      assertEquals(tbl2.getSd().getNumBuckets(), 1);

      assertEquals("Use this for comments etc", tbl2.getSd().getParameters()
          .get("test_param_1"));
      assertEquals("name", tbl2.getSd().getBucketCols().get(0));

      assertNotNull(tbl2.getPartitionKeys());
      assertEquals(2, tbl2.getPartitionKeys().size());
      assertEquals(ColumnType.DATE_TYPE_NAME, tbl2.getPartitionKeys().get(0)
          .getType());
      assertEquals(ColumnType.INT_TYPE_NAME, tbl2.getPartitionKeys().get(1)
          .getType());
      assertEquals("ds", tbl2.getPartitionKeys().get(0).getName());
      assertEquals("hr", tbl2.getPartitionKeys().get(1).getName());

      List<FieldSchema> fieldSchemas = client.getFields(dbName, tblName);
      assertNotNull(fieldSchemas);
      assertEquals(fieldSchemas.size(), tbl.getSd().getCols().size());
      for (FieldSchema fs : tbl.getSd().getCols()) {
        assertTrue("fieldSchemas variable doesn't contain " + fs, fieldSchemas.contains(fs));
      }

      List<FieldSchema> fieldSchemasFull = client.getSchema(dbName, tblName);
      assertNotNull(fieldSchemasFull);
      assertEquals(fieldSchemasFull.size(), tbl.getSd().getCols().size()
          + tbl.getPartitionKeys().size());
      for (FieldSchema fs : tbl.getSd().getCols()) {
        assertTrue("fieldSchemasFull variable doesn't contain " + fs, fieldSchemasFull.contains(fs));
      }
      for (FieldSchema fs : tbl.getPartitionKeys()) {
        assertTrue("fieldSchemasFull variable doesn't contain " + fs, fieldSchemasFull.contains(fs));
      }
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testComplexTable() failed.");
      throw e;
    } finally {
      client.dropTable(dbName, tblName);
      boolean ret = client.dropType(typeName);
      assertTrue("Unable to drop type " + typeName, ret);
      client.dropDatabase(dbName);
    }
  }

  @Test
  public void testTableDatabase() throws Exception {
    String dbName = "testDb";
    String tblName_1 = "testTbl_1";
    String tblName_2 = "testTbl_2";

    try {
      silentDropDatabase(dbName);

      String extWarehouse =  MetastoreConf.getVar(conf, ConfVars.WAREHOUSE_EXTERNAL);
      LOG.info("external warehouse set to:" + extWarehouse);
      if (extWarehouse == null || extWarehouse.trim().isEmpty()) {
        extWarehouse = "/tmp/external";
      }
      String dbLocation =
          extWarehouse + "/_testDB_table_database_";
      String mgdLocation =
          MetastoreConf.getVar(conf, ConfVars.WAREHOUSE) + "/_testDB_table_database_";
      new DatabaseBuilder()
          .setName(dbName)
          .setLocation(dbLocation)
          .setManagedLocation(mgdLocation)
          .create(client, conf);
      Database db = client.getDatabase(dbName);

      Table tbl = new TableBuilder()
          .setDbName(dbName)
          .setTableName(tblName_1)
          .setType(TableType.EXTERNAL_TABLE.name())
          .addCol("name", ColumnType.STRING_TYPE_NAME)
          .addCol("income", ColumnType.INT_TYPE_NAME)
          .addTableParam("EXTERNAL", "TRUE")
          .create(client, conf);

      tbl = client.getTable(dbName, tblName_1);

      Path path = new Path(tbl.getSd().getLocation());
      System.err.println("Table's location " + path + ", Database's location " + db.getLocationUri());
      assertEquals("Table type is expected to be EXTERNAL", TableType.EXTERNAL_TABLE.name(), tbl.getTableType());
      assertEquals("Table location is not a subset of the database location",
          db.getLocationUri(), path.getParent().toString());

    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testTableDatabase() failed.");
      throw e;
    } finally {
      silentDropDatabase(dbName);
    }
  }


  @Test
  public void testGetConfigValue() {

    String val = "value";

    if (!isThriftClient) {
      try {
        assertEquals(client.getConfigValue("hive.key1", val), "value1");
        assertEquals(client.getConfigValue("hive.key2", val), "http://www.example.com");
        assertEquals(client.getConfigValue("hive.key3", val), "");
        assertEquals(client.getConfigValue("hive.key4", val), "0");
        assertEquals(client.getConfigValue("hive.key5", val), val);
        assertEquals(client.getConfigValue(null, val), val);
      } catch (TException e) {
        e.printStackTrace();
        fail();
      }
    }

    boolean threwException = false;
    try {
      // Attempting to get the password should throw an exception
      client.getConfigValue("javax.jdo.option.ConnectionPassword", "password");
    } catch (ConfigValSecurityException e) {
      threwException = true;
    } catch (TException e) {
      e.printStackTrace();
      fail();
    }
    assert (threwException);
  }

  private static void adjust(HiveMetaStoreClient client, Partition part,
      String dbName, String tblName, boolean isThriftClient) throws TException {
    Partition part_get = client.getPartition(dbName, tblName, part.getValues());
    if (isThriftClient) {
      part.setCreateTime(part_get.getCreateTime());
      part.putToParameters(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.DDL_TIME, Long.toString(part_get.getCreateTime()));
    }
    part.setWriteId(part_get.getWriteId());
  }



  private static void silentDropDatabase(String dbName) throws TException {
    try {
      for (String tableName : client.getTables(dbName, "*")) {
        client.dropTable(dbName, tableName);
      }
      client.dropDatabase(dbName);
    } catch (NoSuchObjectException|InvalidOperationException|MetaException e) {
      // NOP
    }
  }

  /**
   * Tests for list partition by filter functionality.
   */

  @Test
  public void testPartitionFilter() throws Exception {
    String dbName = "filterdb";
    String tblName = "filtertbl";

    silentDropDatabase(dbName);

    new DatabaseBuilder()
        .setName(dbName)
        .create(client, conf);

    Table tbl = new TableBuilder()
        .setDbName(dbName)
        .setTableName(tblName)
        .addCol("c1", ColumnType.STRING_TYPE_NAME)
        .addCol("c2", ColumnType.INT_TYPE_NAME)
        .addPartCol("p1", ColumnType.STRING_TYPE_NAME)
        .addPartCol("p2", "varchar(20)")
        .addPartCol("p3", ColumnType.INT_TYPE_NAME)
        .addPartCol("p4", "char(20)")
        .create(client, conf);

    tbl = client.getTable(dbName, tblName);

    add_partition(client, tbl, Lists.newArrayList("p11", "p21", "31", "p41"), "part1");
    add_partition(client, tbl, Lists.newArrayList("p11", "p22", "32", "p42"), "part2");
    add_partition(client, tbl, Lists.newArrayList("p12", "p21", "31", "p43"), "part3");
    add_partition(client, tbl, Lists.newArrayList("p12", "p23", "32", "p43"), "part4");
    add_partition(client, tbl, Lists.newArrayList("p13", "p24", "31", "p44"), "part5");
    add_partition(client, tbl, Lists.newArrayList("p13", "p25", "-33", "p45"), "part6");

    // Test equals operator for strings and integers.
    checkFilter(client, dbName, tblName, "p1 = \"p11\"", 2);
    checkFilter(client, dbName, tblName, "p1 = \"p12\"", 2);
    checkFilter(client, dbName, tblName, "p2 = \"p21\"", 2);
    checkFilter(client, dbName, tblName, "p2 = \"p23\"", 1);
    checkFilter(client, dbName, tblName, "p3 = 31", 3);
    checkFilter(client, dbName, tblName, "p3 = 33", 0);
    checkFilter(client, dbName, tblName, "p3 = -33", 1);
    checkFilter(client, dbName, tblName, "p1 = \"p11\" and p2=\"p22\"", 1);
    checkFilter(client, dbName, tblName, "p1 = \"p11\" or p2=\"p23\"", 3);
    checkFilter(client, dbName, tblName, "p1 = \"p11\" or p1=\"p12\"", 4);
    checkFilter(client, dbName, tblName, "p1 = \"p11\" or p1=\"p12\"", 4);
    checkFilter(client, dbName, tblName, "p1 = \"p11\" or p1=\"p12\"", 4);
    checkFilter(client, dbName, tblName, "p1 = \"p11\" and p3 = 31", 1);
    checkFilter(client, dbName, tblName, "p3 = -33 or p1 = \"p12\"", 3);
    checkFilter(client, dbName, tblName, "p1 = \"p11\" and p4 = \"p41\"", 1);
    checkFilter(client, dbName, tblName, "p1 = \"p12\" and p4 = \"p43\"", 2);

    // Test not-equals operator for strings and integers.
    checkFilter(client, dbName, tblName, "p1 != \"p11\"", 4);
    checkFilter(client, dbName, tblName, "p2 != \"p23\"", 5);
    checkFilter(client, dbName, tblName, "p2 != \"p33\"", 6);
    checkFilter(client, dbName, tblName, "p3 != 32", 4);
    checkFilter(client, dbName, tblName, "p3 != 8589934592", 6);
    checkFilter(client, dbName, tblName, "p1 != \"p11\" and p1 != \"p12\"", 2);
    checkFilter(client, dbName, tblName, "p1 != \"p11\" and p2 != \"p22\"", 4);
    checkFilter(client, dbName, tblName, "p1 != \"p11\" or p2 != \"p22\"", 5);
    checkFilter(client, dbName, tblName, "p1 != \"p12\" and p2 != \"p25\"", 3);
    checkFilter(client, dbName, tblName, "p1 != \"p12\" or p2 != \"p25\"", 6);
    checkFilter(client, dbName, tblName, "p3 != -33 or p1 != \"p13\"", 5);
    checkFilter(client, dbName, tblName, "p1 != \"p11\" and p3 = 31", 2);
    checkFilter(client, dbName, tblName, "p3 != 31 and p1 = \"p12\"", 1);
    checkFilter(client, dbName, tblName, "p1 != \"p11\" and p4 != \"p43\"", 2);
    checkFilter(client, dbName, tblName, "p3 != 31 and p4 = \"p43\"", 1);

    // Test reverse order.
    checkFilter(client, dbName, tblName, "31 != p3 and p1 = \"p12\"", 1);
    checkFilter(client, dbName, tblName, "\"p23\" = p2", 1);

    // Test and/or more...
    checkFilter(client, dbName, tblName,
        "p1 = \"p11\" or (p1=\"p12\" and p2=\"p21\")", 3);
    checkFilter(client, dbName, tblName,
       "p1 = \"p11\" or (p1=\"p12\" and p2=\"p21\") Or " +
       "(p1=\"p13\" aNd p2=\"p24\")", 4);
    //test for and or precedence
    checkFilter(client, dbName, tblName,
       "p1=\"p12\" and (p2=\"p27\" Or p2=\"p21\")", 1);
    checkFilter(client, dbName, tblName,
       "p1=\"p12\" and p2=\"p27\" Or p2=\"p21\"", 2);
    checkFilter(client, dbName, tblName,
        "p1=\"p11\" and (p4=\"p41\" or p4=\"p42\")", 2);

    // Test gt/lt/lte/gte/like for strings.
    checkFilter(client, dbName, tblName, "p1 > \"p12\"", 2);
    checkFilter(client, dbName, tblName, "p1 >= \"p12\"", 4);
    checkFilter(client, dbName, tblName, "p1 < \"p12\"", 2);
    checkFilter(client, dbName, tblName, "p1 <= \"p12\"", 4);
    checkFilter(client, dbName, tblName, "p1 like \"p1%\"", 6);
    checkFilter(client, dbName, tblName, "p2 like \"p%3\"", 1);
    checkFilter(client, dbName, tblName, "p4 like \"p4%\"", 6);

    // Test gt/lt/lte/gte for numbers.
    checkFilter(client, dbName, tblName, "p3 < 0", 1);
    checkFilter(client, dbName, tblName, "p3 >= -33", 6);
    checkFilter(client, dbName, tblName, "p3 > -33", 5);
    checkFilter(client, dbName, tblName, "p3 > 31 and p3 < 32", 0);
    checkFilter(client, dbName, tblName, "p3 > 31 or p3 < 31", 3);
    checkFilter(client, dbName, tblName, "p3 > 30 or p3 < 30", 6);
    checkFilter(client, dbName, tblName, "p3 >= 31 or p3 < -32", 6);
    checkFilter(client, dbName, tblName, "p3 >= 32", 2);
    checkFilter(client, dbName, tblName, "p3 > 32", 0);

    // Test between
    checkFilter(client, dbName, tblName, "p1 between \"p11\" and \"p12\"", 4);
    checkFilter(client, dbName, tblName, "p1 not between \"p11\" and \"p12\"", 2);
    checkFilter(client, dbName, tblName, "p3 not between 0 and 2", 6);
    checkFilter(client, dbName, tblName, "p3 between 31 and 32", 5);
    checkFilter(client, dbName, tblName, "p3 between 32 and 31", 0);
    checkFilter(client, dbName, tblName, "p3 between -32 and 34 and p3 not between 31 and 32", 0);
    checkFilter(client, dbName, tblName, "p3 between 1 and 3 or p3 not between 1 and 3", 6);
    checkFilter(client, dbName, tblName,
        "p3 between 31 and 32 and p1 between \"p12\" and \"p14\"", 3);
    checkFilter(client, dbName, tblName, "p4 between \"p41\" and \"p44\"", 5);

    // Test in
    checkFilter(client, dbName, tblName, "(p1) in (\"p11\", \"p12\")", 4);
    checkFilter(client, dbName, tblName, "(p2) in (\"p21\", \"p25\")", 3);
    checkFilter(client, dbName, tblName, "(p3) not in (31, 33)", 3);
    checkFilter(client, dbName, tblName, "(p4) not in ('p43', 'p44')", 3);

    // Test multi-in
    checkFilter(client, dbName, tblName, "(struct (p1, p2)) in (const struct ('p11', 'p22'), const struct ('p12', 'p22'))", 1);
    checkFilter(client, dbName, tblName, "(struct (p1, p3)) not in (struct ('p11', 31), struct ('p12', 33))", 5);

    //Test for setting the maximum partition count
    List<Partition> partitions = client.listPartitionsByFilter(dbName,
        tblName, "p1 >= \"p12\"", (short) 2);
    assertEquals("User specified row limit for partitions",
        2, partitions.size());

    //Negative tests
    Exception me = null;
    try {
      client.listPartitionsByFilter(dbName,
          tblName, "p3 >= \"p12\"", (short) -1);
    } catch(MetaException e) {
      me = e;
    }
    assertNotNull(me);
    assertTrue("Filter on int partition key", me.getMessage().contains(
          "Filtering is supported only on partition keys of type string"));

    me = null;
    try {
      client.listPartitionsByFilter(dbName,
          tblName, "c1 >= \"p12\"", (short) -1);
    } catch(MetaException e) {
      me = e;
    }
    assertNotNull(me);
    assertTrue("Filter on invalid key", me.getMessage().contains(
          "<c1> is not a partitioning key for the table"));

    me = null;
    try {
      client.listPartitionsByFilter(dbName,
          tblName, "c1 >= ", (short) -1);
    } catch(MetaException e) {
      me = e;
    }
    assertNotNull(me);
    assertTrue("Invalid filter string", me.getMessage().contains(
          "Error parsing partition filter"));

    me = null;
    try {
      client.listPartitionsByFilter("invDBName",
          "invTableName", "p1 = \"p11\"", (short) -1);
    } catch(NoSuchObjectException e) {
      me = e;
    }
    assertNotNull(me);
    assertTrue("NoSuchObject exception", me.getMessage().contains(
          "Specified catalog.database.table does not exist : hive.invdbname.invtablename"));

    client.dropTable(dbName, tblName);
    client.dropDatabase(dbName);
  }


  /**
   * Test filtering on table with single partition
   */
  @Test
  public void testFilterSinglePartition() throws Exception {
      String dbName = "filterdb";
      String tblName = "filtertbl";

      List<String> vals = new ArrayList<>(1);
      vals.add("p11");
      List <String> vals2 = new ArrayList<>(1);
      vals2.add("p12");
      List <String> vals3 = new ArrayList<>(1);
      vals3.add("p13");

      silentDropDatabase(dbName);

      new DatabaseBuilder()
          .setName(dbName)
          .create(client, conf);

      Table tbl = new TableBuilder()
          .setDbName(dbName)
          .setTableName(tblName)
          .addCol("c1", ColumnType.STRING_TYPE_NAME)
          .addCol("c2", ColumnType.INT_TYPE_NAME)
          .addPartCol("p1", ColumnType.STRING_TYPE_NAME)
          .create(client, conf);

      tbl = client.getTable(dbName, tblName);

      add_partition(client, tbl, vals, "part1");
      add_partition(client, tbl, vals2, "part2");
      add_partition(client, tbl, vals3, "part3");

      checkFilter(client, dbName, tblName, "p1 = \"p12\"", 1);
      checkFilter(client, dbName, tblName, "p1 < \"p12\"", 1);
      checkFilter(client, dbName, tblName, "p1 > \"p12\"", 1);
      checkFilter(client, dbName, tblName, "p1 >= \"p12\"", 2);
      checkFilter(client, dbName, tblName, "p1 <= \"p12\"", 2);
      checkFilter(client, dbName, tblName, "p1 <> \"p12\"", 2);
      checkFilter(client, dbName, tblName, "p1 like \"p1%\"", 3);
      checkFilter(client, dbName, tblName, "p1 like \"p%2\"", 1);

      client.dropTable(dbName, tblName);
      client.dropDatabase(dbName);
  }

  /**
   * Test filtering based on the value of the last partition
   */
  @Test
  public void testFilterLastPartition() throws Exception {
      String dbName = "filterdb";
      String tblName = "filtertbl";

      List<String> vals = new ArrayList<>(2);
      vals.add("p11");
      vals.add("p21");
      List <String> vals2 = new ArrayList<>(2);
      vals2.add("p11");
      vals2.add("p22");
      List <String> vals3 = new ArrayList<>(2);
      vals3.add("p12");
      vals3.add("p21");

      cleanUp(dbName, tblName, null);

      createDb(dbName);

      Table tbl = new TableBuilder()
          .setDbName(dbName)
          .setTableName(tblName)
          .addCol("c1", ColumnType.STRING_TYPE_NAME)
          .addCol("c2", ColumnType.INT_TYPE_NAME)
          .addPartCol("p1", ColumnType.STRING_TYPE_NAME)
          .addPartCol("p2", ColumnType.STRING_TYPE_NAME)
          .create(client, conf);

      tbl = client.getTable(dbName, tblName);

      add_partition(client, tbl, vals, "part1");
      add_partition(client, tbl, vals2, "part2");
      add_partition(client, tbl, vals3, "part3");

      checkFilter(client, dbName, tblName, "p2 = \"p21\"", 2);
      checkFilter(client, dbName, tblName, "p2 < \"p23\"", 3);
      checkFilter(client, dbName, tblName, "p2 > \"p21\"", 1);
      checkFilter(client, dbName, tblName, "p2 >= \"p21\"", 3);
      checkFilter(client, dbName, tblName, "p2 <= \"p21\"", 2);
      checkFilter(client, dbName, tblName, "p2 <> \"p12\"", 3);
      checkFilter(client, dbName, tblName, "p2 != \"p12\"", 3);

      try {
        checkFilter(client, dbName, tblName, "p2 !< 'dd'", 0);
        fail("Invalid operator not detected");
      } catch (MetaException e) {
        // expected exception due to lexer error
      }

      cleanUp(dbName, tblName, null);
  }

  /**
   * Test "like" filtering on table with single partition.
   */
  @Test
  public void testPartitionFilterLike() throws Exception {
    String dbName = "filterdb";
    String tblName = "filtertbl";

    List<String> vals = new ArrayList<>(1);
    vals.add("abc");
    List <String> vals2 = new ArrayList<>(1);
    vals2.add("d_\\\\%ae");
    List <String> vals3 = new ArrayList<>(1);
    vals3.add("af%");

    silentDropDatabase(dbName);

    new DatabaseBuilder()
        .setName(dbName)
        .create(client, conf);

    Table tbl = new TableBuilder()
        .setDbName(dbName)
        .setTableName(tblName)
        .addCol("c1", ColumnType.STRING_TYPE_NAME)
        .addCol("c2", ColumnType.INT_TYPE_NAME)
        .addPartCol("p1", ColumnType.STRING_TYPE_NAME)
        .create(client, conf);

    tbl = client.getTable(dbName, tblName);

    add_partition(client, tbl, vals, "part1");
    add_partition(client, tbl, vals2, "part2");
    add_partition(client, tbl, vals3, "part3");

    checkFilter(client, dbName, tblName, "p1 like \"a%\"", 2);
    checkFilter(client, dbName, tblName, "p1 like \"%a%\"", 3);
    checkFilter(client, dbName, tblName, "p1 like \"%a\"", 0);
    checkFilter(client, dbName, tblName, "p1 like \"a%c\"", 1);
    checkFilter(client, dbName, tblName, "p1 like \"%a%c%\"", 1);
    checkFilter(client, dbName, tblName, "p1 like \"%_b%\"", 1);
    checkFilter(client, dbName, tblName, "p1 like \"%b_\"", 1);
    checkFilter(client, dbName, tblName, "p1 like \"%c\"", 1);
    checkFilter(client, dbName, tblName, "p1 like \"%c%\"", 1);
    checkFilter(client, dbName, tblName, "p1 like \"c%\"", 0);
    checkFilter(client, dbName, tblName, "p1 like \"%\\_%\"", 1);
    checkFilter(client, dbName, tblName, "p1 like \"%\\%%\"", 2);
    checkFilter(client, dbName, tblName, "p1 like \"abc\"", 1);
    checkFilter(client, dbName, tblName, "p1 like \"%\"", 3);
    checkFilter(client, dbName, tblName, "p1 like \"_\"", 0);
    checkFilter(client, dbName, tblName, "p1 like \"___\"", 2);
    checkFilter(client, dbName, tblName, "p1 like \"%%%\"", 3);
    checkFilter(client, dbName, tblName, "p1 like \"%\\\\\\\\%\"", 1);

    client.dropTable(dbName, tblName);
    client.dropDatabase(dbName);
  }

  private void checkFilter(HiveMetaStoreClient client, String dbName,
        String tblName, String filter, int expectedCount) throws TException {
    LOG.debug("Testing filter: " + filter);
    List<Partition> partitions = client.listPartitionsByFilter(dbName,
            tblName, filter, (short) -1);

    assertEquals("Partition count expected for filter " + filter,
            expectedCount, partitions.size());
  }

  private void add_partition(HiveMetaStoreClient client, Table table,
      List<String> vals, String location) throws TException {
    Partition part = makePartitionObject(table.getDbName(), table.getTableName(), vals, table, location);
    client.add_partition(part);
  }

  /**
   * Tests {@link SynchronizedMetaStoreClient#newSynchronizedClient}.  Does not
   * actually test multithreading, but does verify that the proxy
   * at least works correctly.
   */
  @Test
  public void testSynchronized() throws Exception {
    int currentNumberOfDbs = client.getAllDatabases().size();

    IMetaStoreClient synchronizedClient = SynchronizedMetaStoreClient.newSynchronizedClient(client);
    List<String> databases = synchronizedClient.getAllDatabases();
    assertEquals(currentNumberOfDbs, databases.size());
  }

  @Test
  public void testTableFilter() throws Exception {
    try {
      String dbName = "testTableFilter";
      String owner1 = "testOwner1";
      String owner2 = "testOwner2";
      int lastAccessTime1 = 90;
      int lastAccessTime2 = 30;
      String tableName1 = "table1";
      String tableName2 = "table2";
      String tableName3 = "table3";

      client.dropTable(dbName, tableName1);
      client.dropTable(dbName, tableName2);
      client.dropTable(dbName, tableName3);
      silentDropDatabase(dbName);
      new DatabaseBuilder()
          .setName(dbName)
          .setDescription("Alter Partition Test database")
          .create(client, conf);

      Table table1 = createTableForTestFilter(dbName,tableName1, owner1, lastAccessTime1, true);
      Table table2 = createTableForTestFilter(dbName,tableName2, owner2, lastAccessTime2, true);
      Table table3 = createTableForTestFilter(dbName,tableName3, owner1, lastAccessTime2, false);

      List<String> tableNames;
      String filter;
      //test owner
      //owner like ".*Owner.*" and owner like "test.*"
      filter = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_OWNER +
          " like \".*Owner.*\" and " +
          org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_OWNER +
          " like  \"test.*\"";
      tableNames = client.listTableNamesByFilter(dbName, filter, (short)-1);
      assertEquals(tableNames.size(), 3);
      assert(tableNames.contains(table1.getTableName()));
      assert(tableNames.contains(table2.getTableName()));
      assert(tableNames.contains(table3.getTableName()));

      //owner = "testOwner1"
      filter = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_OWNER +
          " = \"testOwner1\"";

      tableNames = client.listTableNamesByFilter(dbName, filter, (short)-1);
      assertEquals(2, tableNames.size());
      assert(tableNames.contains(table1.getTableName()));
      assert(tableNames.contains(table3.getTableName()));

      //lastAccessTime < 90
      filter = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_LAST_ACCESS +
          " < 90";

      tableNames = client.listTableNamesByFilter(dbName, filter, (short)-1);
      assertEquals(2, tableNames.size());
      assert(tableNames.contains(table2.getTableName()));
      assert(tableNames.contains(table3.getTableName()));

      //lastAccessTime > 90
      filter = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_LAST_ACCESS +
      " > 90";

      tableNames = client.listTableNamesByFilter(dbName, filter, (short)-1);
      assertEquals(0, tableNames.size());

      //test params
      //test_param_2 = "50"
      filter = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS +
          "test_param_2 LIKE \"50\"";

      tableNames = client.listTableNamesByFilter(dbName, filter, (short)-1);
      assertEquals(2, tableNames.size());
      assert(tableNames.contains(table1.getTableName()));
      assert(tableNames.contains(table2.getTableName()));

      //test_param_2 = "75"
      filter = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS +
          "test_param_2 LIKE \"75\"";

      tableNames = client.listTableNamesByFilter(dbName, filter, (short)-1);
      assertEquals(0, tableNames.size());

      //key_dne = "50"
      filter = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS +
          "key_dne LIKE \"50\"";

      tableNames = client.listTableNamesByFilter(dbName, filter, (short)-1);
      assertEquals(0, tableNames.size());

      //test_param_1 != "yellow"
      filter = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS +
          "test_param_1 NOT LIKE \"yellow\"";

      // Commenting as part of HIVE-12274 != and <> are not supported for CLOBs
      // tableNames = client.listTableNamesByFilter(dbName, filter, (short) 2);
      // assertEquals(2, tableNames.size());

      filter = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS +
          "test_param_1 NOT LIKE \"yellow\"";

      // tableNames = client.listTableNamesByFilter(dbName, filter, (short) 2);
      // assertEquals(2, tableNames.size());

      //owner = "testOwner1" and (lastAccessTime = 30 or test_param_1 = "hi")
      filter = org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_OWNER +
        " = \"testOwner1\" and (" +
        org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_LAST_ACCESS +
        " = 30 or " +
        org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS +
        "test_param_1 LIKE \"hi\")";
      tableNames = client.listTableNamesByFilter(dbName, filter, (short)-1);

      assertEquals(2, tableNames.size());
      assert(tableNames.contains(table1.getTableName()));
      assert(tableNames.contains(table3.getTableName()));

      //Negative tests
      Exception me = null;
      try {
        filter = "badKey = \"testOwner1\"";
        tableNames = client.listTableNamesByFilter(dbName, filter, (short) -1);
      } catch(MetaException e) {
        me = e;
      }
      assertNotNull(me);
      assertTrue("Bad filter key test", me.getMessage().contains(
            "Invalid key name in filter"));

      client.dropTable(dbName, tableName1);
      client.dropTable(dbName, tableName2);
      client.dropTable(dbName, tableName3);
      client.dropDatabase(dbName);
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testTableFilter() failed.");
      throw e;
    }
  }

  private Table createTableForTestFilter(String dbName, String tableName, String owner,
    int lastAccessTime, boolean hasSecondParam) throws Exception {

    Map<String, String> tableParams =  new HashMap<>();
    tableParams.put("test_param_1", "hi");
    if(hasSecondParam) {
      tableParams.put("test_param_2", "50");
    }

    Table tbl = new TableBuilder()
        .setDbName(dbName)
        .setTableName(tableName)
        .addCol("name", ColumnType.STRING_TYPE_NAME)
        .addCol("income", ColumnType.INT_TYPE_NAME)
        .addPartCol("ds", ColumnType.STRING_TYPE_NAME)
        .addPartCol("hr", ColumnType.INT_TYPE_NAME)
        .setTableParams(tableParams)
        .setOwner(owner)
        .setLastAccessTime(lastAccessTime)
        .create(client, conf);

    if (isThriftClient) {
      // the createTable() above does not update the location in the 'tbl'
      // object when the client is a thrift client and the code below relies
      // on the location being present in the 'tbl' object - so get the table
      // from the metastore
      tbl = client.getTable(dbName, tableName);
    }
    return tbl;
  }
  /**
   * Verify that if another  client, either a metastore Thrift server or  a Hive CLI instance
   * renames a table recently created by this instance, and hence potentially in its cache, the
   * current instance still sees the change.
   */
  @Test
  public void testConcurrentMetastores() throws Exception {
    String dbName = "concurrentdb";
    String tblName = "concurrenttbl";
    String renameTblName = "rename_concurrenttbl";

    try {
      cleanUp(dbName, tblName, null);

      createDb(dbName);

      Table tbl1 = new TableBuilder()
          .setDbName(dbName)
          .setTableName(tblName)
          .addCol("c1", ColumnType.STRING_TYPE_NAME)
          .addCol("c2", ColumnType.INT_TYPE_NAME)
          .create(client, conf);

      // get the table from the client, verify the name is correct
      Table tbl2 = client.getTable(dbName, tblName);

      assertEquals("Client returned table with different name.", tbl2.getTableName(), tblName);

      // Simulate renaming via another metastore Thrift server or another Hive CLI instance
      updateTableNameInDB(tblName, renameTblName);

      // get the table from the client again, verify the name has been updated
      Table tbl3 = client.getTable(dbName, renameTblName);

      assertEquals("Client returned table with different name after rename.",
          tbl3.getTableName(), renameTblName);

    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testConcurrentMetastores() failed.");
      throw e;
    } finally {
      silentDropDatabase(dbName);
    }
  }

  @Test
  public void testSimpleFunction() throws Exception {
    String dbName = "test_db";
    String funcName = "test_func";
    String className = "org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper";
    String owner = "test_owner";
    final int N_FUNCTIONS = 5;
    PrincipalType ownerType = PrincipalType.USER;
    int createTime = (int) (System.currentTimeMillis() / 1000);
    FunctionType funcType = FunctionType.JAVA;

    try {
      cleanUp(dbName, null, null);
      for (Function f : client.getAllFunctions().getFunctions()) {
        client.dropFunction(f.getDbName(), f.getFunctionName());
      }

      createDb(dbName);

      for (int i = 0; i < N_FUNCTIONS; i++) {
        createFunction(dbName, funcName + "_" + i, className, owner, ownerType, createTime, funcType, null);
      }

      // Try the different getters

      // getFunction()
      Function func = client.getFunction(dbName, funcName + "_0");
      assertEquals("function db name", dbName, func.getDbName());
      assertEquals("function name", funcName + "_0", func.getFunctionName());
      assertEquals("function class name", className, func.getClassName());
      assertEquals("function owner name", owner, func.getOwnerName());
      assertEquals("function owner type", PrincipalType.USER, func.getOwnerType());
      assertEquals("function type", funcType, func.getFunctionType());
      List<ResourceUri> resources = func.getResourceUris();
      assertTrue("function resources: " + resources, resources == null || resources.size() == 0);

      boolean gotException = false;
      try {
        func = client.getFunction(dbName, "nonexistent_func");
      } catch (NoSuchObjectException e) {
        // expected failure
        gotException = true;
      }
      assertEquals(true, gotException);

      // getAllFunctions()
      GetAllFunctionsResponse response = client.getAllFunctions();
      List<Function> allFunctions = response.getFunctions();
      assertEquals(N_FUNCTIONS, allFunctions.size());
      assertEquals(funcName + "_3", allFunctions.get(3).getFunctionName());

      // getFunctions()
      List<String> funcs = client.getFunctions(dbName, "*_func_*");
      assertEquals(N_FUNCTIONS, funcs.size());
      assertEquals(funcName + "_0", funcs.get(0));

      funcs = client.getFunctions(dbName, "nonexistent_func");
      assertEquals(0, funcs.size());

      // dropFunction()
      for (int i = 0; i < N_FUNCTIONS; i++) {
        client.dropFunction(dbName, funcName + "_" + i);
      }

      // Confirm that the function is now gone
      funcs = client.getFunctions(dbName, funcName);
      assertEquals(0, funcs.size());
      response = client.getAllFunctions();
      allFunctions = response.getFunctions();
      assertEquals(0, allFunctions.size());
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testConcurrentMetastores() failed.");
      throw e;
    } finally {
      silentDropDatabase(dbName);
    }
  }

  @Test
  public void testFunctionWithResources() throws Exception {
    String dbName = "test_db2";
    String funcName = "test_func";
    String className = "org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper";
    String owner = "test_owner";
    PrincipalType ownerType = PrincipalType.USER;
    int createTime = (int) (System.currentTimeMillis() / 1000);
    FunctionType funcType = FunctionType.JAVA;
    List<ResourceUri> resList = new ArrayList<>();
    resList.add(new ResourceUri(ResourceType.JAR, "hdfs:///tmp/jar1.jar"));
    resList.add(new ResourceUri(ResourceType.FILE, "hdfs:///tmp/file1.txt"));
    resList.add(new ResourceUri(ResourceType.ARCHIVE, "hdfs:///tmp/archive1.tgz"));

    try {
      cleanUp(dbName, null, null);

      createDb(dbName);

      createFunction(dbName, funcName, className, owner, ownerType, createTime, funcType, resList);

      // Try the different getters

      // getFunction()
      Function func = client.getFunction(dbName, funcName);
      assertEquals("function db name", dbName, func.getDbName());
      assertEquals("function name", funcName, func.getFunctionName());
      assertEquals("function class name", className, func.getClassName());
      assertEquals("function owner name", owner, func.getOwnerName());
      assertEquals("function owner type", PrincipalType.USER, func.getOwnerType());
      assertEquals("function type", funcType, func.getFunctionType());
      List<ResourceUri> resources = func.getResourceUris();
      assertEquals("Resource list size", resList.size(), resources.size());
      for (ResourceUri res : resources) {
        assertTrue("Matching resource " + res.getResourceType() + " " + res.getUri(),
            resList.indexOf(res) >= 0);
      }

      client.dropFunction(dbName, funcName);
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testConcurrentMetastores() failed.");
      throw e;
    } finally {
      silentDropDatabase(dbName);
    }
  }

  /**
   * This method simulates another Hive metastore renaming a table, by accessing the db and
   * updating the name.
   *
   * Unfortunately, derby cannot be run in two different JVMs simultaneously, but the only way
   * to rename without having it put in this client's cache is to run a metastore in a separate JVM,
   * so this simulation is required.
   */
  private void updateTableNameInDB(String oldTableName, String newTableName) throws SQLException {
    String connectionStr = MetastoreConf.getVar(conf, ConfVars.CONNECT_URL_KEY);

    Connection conn = DriverManager.getConnection(connectionStr);
    PreparedStatement stmt = conn.prepareStatement("UPDATE TBLS SET tbl_name = '" +
        newTableName + "' WHERE tbl_name = '" + oldTableName + "'");
    stmt.executeUpdate();
  }

  protected void cleanUp(String dbName, String tableName, String typeName) throws Exception {
    if(dbName != null && tableName != null) {
      client.dropTable(dbName, tableName);
    }
    if(dbName != null) {
      silentDropDatabase(dbName);
    }
    if(typeName != null) {
      client.dropType(typeName);
    }
  }

  protected Database createDb(String dbName) throws Exception {
    if(null == dbName) { return null; }
    return new DatabaseBuilder()
        .setName(dbName)
        .create(client, conf);
  }

  private Type createType(String typeName, Map<String, String> fields) throws Throwable {
    Type typ1 = new Type();
    typ1.setName(typeName);
    typ1.setFields(new ArrayList<>(fields.size()));
    for(String fieldName : fields.keySet()) {
      typ1.getFields().add(
          new FieldSchema(fieldName, fields.get(fieldName), ""));
    }
    client.createType(typ1);
    return typ1;
  }

  /**
   * Creates a simple table under specified database
   * @param dbName    the database name that the table will be created under
   * @param tableName the table name to be created
   */

  private Table createTable(String dbName, String tableName) throws TException {
    return new TableBuilder()
        .setDbName(dbName)
        .setTableName(tableName)
        .addCol("foo", "string")
        .addCol("bar", "string")
        .create(client, conf);
  }

  public static SourceTable createSourceTable(Table table) {
    SourceTable sourceTable = new SourceTable();
    sourceTable.setTable(table);
    sourceTable.setInsertedCount(0L);
    sourceTable.setUpdatedCount(0L);
    sourceTable.setDeletedCount(0L);
    return sourceTable;
  }

  private void createMaterializedView(String dbName, String tableName, Set<Table> tablesUsed)
      throws TException {
    Set<SourceTable> sourceTables = new HashSet<>(tablesUsed.size());
    for (Table table : tablesUsed) {
      sourceTables.add(createSourceTable(table));
    }

    Table t = new TableBuilder()
        .setDbName(dbName)
        .setTableName(tableName)
        .setType(TableType.MATERIALIZED_VIEW.name())
        .addMaterializedViewReferencedTables(sourceTables)
        .addCol("foo", "string")
        .addCol("bar", "string")
        .create(client, conf);
  }

  private List<Partition> createPartitions(String dbName, Table tbl,
      List<List<String>> values)  throws Throwable {
    int i = 1;
    List<Partition> partitions = new ArrayList<>();
    for(List<String> vals : values) {
      Partition part = makePartitionObject(dbName, tbl.getTableName(), vals, tbl, "/part"+i);
      i++;
      // check if the partition exists (it shouldn't)
      boolean exceptionThrown = false;
      try {
        Partition p = client.getPartition(dbName, tbl.getTableName(), vals);
      } catch(Exception e) {
        assertEquals("partition should not have existed",
            NoSuchObjectException.class, e.getClass());
        exceptionThrown = true;
      }
      assertTrue("getPartition() should have thrown NoSuchObjectException", exceptionThrown);
      Partition retp = client.add_partition(part);
      assertNotNull("Unable to create partition " + part, retp);
      partitions.add(retp);
    }
    return partitions;
  }

  protected List<Partition> createMultiPartitionTableSchema(String dbName, String tblName,
      String typeName, List<List<String>> values) throws Throwable {
    return createMultiPartitionTableSchema(null, dbName, tblName, typeName, values);
  }
  private List<Partition> createMultiPartitionTableSchema(String catName, String dbName, String tblName,
      String typeName, List<List<String>> values) throws Throwable {
    createDb(dbName);

    Map<String, String> fields = new HashMap<>();
    fields.put("name", ColumnType.STRING_TYPE_NAME);
    fields.put("income", ColumnType.INT_TYPE_NAME);

    Table tbl = new TableBuilder()
        .setDbName(dbName)
        .setTableName(tblName)
        .setCatName(catName)
        .addCol("name", ColumnType.STRING_TYPE_NAME)
        .addCol("income", ColumnType.INT_TYPE_NAME)
        .addPartCol("ds", ColumnType.STRING_TYPE_NAME)
        .addPartCol("hr", ColumnType.STRING_TYPE_NAME)
        .create(client, conf);

    if (isThriftClient) {
      // the createTable() above does not update the location in the 'tbl'
      // object when the client is a thrift client and the code below relies
      // on the location being present in the 'tbl' object - so get the table
      // from the metastore
      tbl = client.getTable(dbName, tblName);
    }

    return createPartitions(dbName, tbl, values);
  }

  @Test
  public void testDBOwner() throws TException {
    Database db = client.getDatabase(Warehouse.DEFAULT_DATABASE_NAME);
    assertEquals(db.getOwnerName(), HMSHandler.PUBLIC);
    assertEquals(db.getOwnerType(), PrincipalType.ROLE);
  }

  /**
   * Test changing owner and owner type of a database
   */
  @Test
  public void testDBOwnerChange() throws TException {
    final String dbName = "alterDbOwner";
    final String user1 = "user1";
    final String user2 = "user2";
    final String role1 = "role1";

    silentDropDatabase(dbName);
    Database db = new DatabaseBuilder()
        .setName(dbName)
        .setOwnerName(user1)
        .setOwnerType(PrincipalType.USER)
        .create(client, conf);

    checkDbOwnerType(dbName, user1, PrincipalType.USER);

    db.setOwnerName(user2);
    client.alterDatabase(dbName, db);
    checkDbOwnerType(dbName, user2, PrincipalType.USER);

    db.setOwnerName(role1);
    db.setOwnerType(PrincipalType.ROLE);
    client.alterDatabase(dbName, db);
    checkDbOwnerType(dbName, role1, PrincipalType.ROLE);

  }

  /**
   * Test table objects can be retrieved in batches
   */
  @Test
  public void testGetTableObjects() throws Exception {
    String dbName = "db";
    List<String> tableNames = Arrays.asList("table1", "table2", "table3", "table4", "table5");

    // Setup
    silentDropDatabase(dbName);

    Set<Table> tablesUsed = new HashSet<>();
    new DatabaseBuilder()
        .setName(dbName)
        .create(client, conf);
    for (String tableName : tableNames) {
      tablesUsed.add(createTable(dbName, tableName));
    }
    createMaterializedView(dbName, "mv1", tablesUsed);

    // Test
    List<Table> tableObjs = client.getTableObjectsByName(dbName, tableNames);

    // Verify
    assertEquals(tableNames.size(), tableObjs.size());
    for(Table table : tableObjs) {
      assertTrue("tableNames doesn't contain " + table.getTableName().toLowerCase() + ", " + tableNames,
          tableNames.contains(table.getTableName().toLowerCase()));
    }

    // Cleanup
    client.dropDatabase(dbName, true, true, true);
  }

  @Test
  public void testDropDatabaseWithCustomPartitionPath() throws Exception {
    String dbName = "dropdb";
    String tblName1 = "droptbl1";
    String tblName2 = "droptbl2";
    String tblName3 = "droptbl3";

    // A new client to support drop these 3 tables in one batch
    Configuration newConf = MetastoreConf.newMetastoreConf(new Configuration(conf));
    MetastoreConf.setLongVar(newConf, ConfVars.BATCH_RETRIEVE_MAX, 3);
    IMetaStoreClient client = RetryingMetaStoreClient.getProxy(newConf, getHookLoader(), HiveMetaStoreClient.class.getName());

    // Setup
    silentDropDatabase(dbName);
    new DatabaseBuilder()
        .setName(dbName)
        .create(client, conf);

    new TableBuilder()
        .setDbName(dbName)
        .setTableName(tblName1)
        .addCol("c1", ColumnType.STRING_TYPE_NAME)
        .addPartCol("p1", ColumnType.STRING_TYPE_NAME)
        .create(client, conf);

    new TableBuilder()
        .setDbName(dbName)
        .setTableName(tblName2)
        .addCol("c1", ColumnType.STRING_TYPE_NAME)
        .addPartCol("p1", ColumnType.STRING_TYPE_NAME)
        .create(client, conf);

    new TableBuilder()
        .setDbName(dbName)
        .setTableName(tblName3)
        .addCol("c1", ColumnType.STRING_TYPE_NAME)
        .create(client, conf);

    Path rootPath = warehouse.getWhRoot();
    Path path1 = new Path(rootPath, tblName1);
    Table tbl1 = client.getTable(dbName, tblName1);
    Table tbl2 = client.getTable(dbName, tblName2);
    Table tbl3 = client.getTable(dbName, tblName3);
    List<String> vals = Lists.newArrayList("p1");

    // partition1 whose path does not belong to table1
    Partition part1 = makePartitionObjectWithAbsoluteLocation(dbName, tblName1, vals, tbl1, path1.toString());
    client.add_partition(part1);

    // partition2 whose path belongs to table2
    Partition part2 = makePartitionObject(dbName, tblName2, vals, tbl2, "/part2");
    client.add_partition(part2);

    client.dropDatabase(dbName, true, true, true);
    FileSystem fs = FileSystem.get(path1.toUri(), conf);
    assertFalse(fs.exists(new Path(tbl1.getSd().getLocation())));
    assertFalse(fs.exists(new Path(tbl2.getSd().getLocation())));
    assertFalse(fs.exists(new Path(tbl3.getSd().getLocation())));
    assertFalse(fs.exists(path1));

    client.close();
  }

  @Test
  public void testDropDatabaseCascadeMVMultiDB() throws Exception {
    String dbName1 = "db1";
    String tableName1 = "table1";
    String dbName2 = "db2";
    String tableName2 = "table2";
    String mvName = "mv1";

    // Setup
    silentDropDatabase(dbName1);
    silentDropDatabase(dbName2);

    Database db1 = new Database();
    db1.setName(dbName1);
    client.createDatabase(db1);
    Table table1 = createTable(dbName1, tableName1);
    Database db2 = new Database();
    db2.setName(dbName2);
    client.createDatabase(db2);
    Table table2 = createTable(dbName2, tableName2);

    createMaterializedView(dbName2, mvName, Sets.newHashSet(table1, table2));

    boolean exceptionFound = false;
    try {
      // Cannot drop db1 because mv1 uses one of its tables
      // TODO: Error message coming from metastore is currently not very concise
      // (foreign key violation), we should make it easily understandable
      client.dropDatabase(dbName1, true, true, true);
    } catch (Exception e) {
      exceptionFound = true;
    }
    assertTrue(exceptionFound);

    client.dropDatabase(dbName2, true, true, true);
    client.dropDatabase(dbName1, true, true, true);
  }

  @Test
  public void testDBLocationChange() throws IOException, TException {
    final String dbName = "alterDbLocation";
    String defaultUri = MetastoreConf.getVar(conf, ConfVars.WAREHOUSE) + "/default_location.db";
    String newUri = MetastoreConf.getVar(conf, ConfVars.WAREHOUSE) + "/new_location.db";

    new DatabaseBuilder()
        .setName(dbName)
        .setLocation(defaultUri)
        .create(client, conf);

    Database db = client.getDatabase(dbName);

    assertEquals("Incorrect default location of the database",
        warehouse.getDnsPath(new Path(defaultUri)).toString(), db.getLocationUri());

    db.setLocationUri(newUri);
    client.alterDatabase(dbName, db);

    db = client.getDatabase(dbName);

    assertEquals("Incorrect new location of the database",
        warehouse.getDnsPath(new Path(newUri)).toString(), db.getLocationUri());

    client.dropDatabase(dbName);
    silentDropDatabase(dbName);
  }

  private void checkDbOwnerType(String dbName, String ownerName, PrincipalType ownerType)
      throws TException {
    Database db = client.getDatabase(dbName);
    assertEquals("Owner name", ownerName, db.getOwnerName());
    assertEquals("Owner type", ownerType, db.getOwnerType());
  }

  private void createFunction(String dbName, String funcName, String className,
      String ownerName, PrincipalType ownerType, int createTime,
      org.apache.hadoop.hive.metastore.api.FunctionType functionType, List<ResourceUri> resources)
          throws Exception {
    Function func = new Function(funcName, dbName, className,
        ownerName, ownerType, createTime, functionType, resources);
    client.createFunction(func);
  }

  @Test
  public void testRetriableClientWithConnLifetime() throws Exception {

    Configuration newConf = MetastoreConf.newMetastoreConf(new Configuration(this.conf));
    MetastoreConf.setTimeVar(newConf, ConfVars.CLIENT_SOCKET_LIFETIME, 4, TimeUnit.SECONDS);
    MetaStoreTestUtils.setConfForStandloneMode(newConf);
    long timeout = 5 * 1000; // Lets use a timeout more than the socket lifetime to simulate a reconnect

    // Test a normal retriable client
    IMetaStoreClient client = RetryingMetaStoreClient.getProxy(newConf, getHookLoader(), HiveMetaStoreClient.class.getName());
    client.getAllDatabases();
    client.close();

    // Connect after the lifetime, there should not be any failures
    client = RetryingMetaStoreClient.getProxy(newConf, getHookLoader(), HiveMetaStoreClient.class.getName());
    Thread.sleep(timeout);
    client.getAllDatabases();
    client.close();
  }

  /**
   * This method checks if persistence manager cache is cleaned up properly under some circumstances.
   * There is a chance that cleanup process removes elements created not only by this test, so we don't check
   * exact equality, instead check that no new elements can be seen in the cache (so newSet.removeAll(oldSet) should be empty).
   */
  @Test
  public void testJDOPersistenceManagerCleanup() throws Exception {
    if (isThriftClient == false) {
      return;
    }

    Set<JDOPersistenceManager> objectsBeforeUse = new HashSet<>(getJDOPersistenceManagerCache());
    HiveMetaStoreClient closingClient = new HiveMetaStoreClient(conf);
    closingClient.getAllDatabases();
    closingClient.close();

    MetaStoreTestUtils.waitForAssertion("Checking pm cachesize after client close", () -> {
      Set<JDOPersistenceManager> objectsAfterClose = new HashSet<>(getJDOPersistenceManagerCache());
      objectsAfterClose.removeAll(objectsBeforeUse);

      assertEquals("new objects left in the cache (after closing client)", 0, objectsAfterClose.size());

    }, 500, 30000);

    Set<JDOPersistenceManager> objectsAfterClose = new HashSet<>(getJDOPersistenceManagerCache());
    HiveMetaStoreClient nonClosingClient = new HiveMetaStoreClient(conf);
    nonClosingClient.getAllDatabases();
    // Drop connection without calling close. HMS thread deleteContext
    // will trigger cleanup
    nonClosingClient.getThriftClient().getTTransport().close();

    MetaStoreTestUtils.waitForAssertion("Checking pm cachesize after transport close", () -> {
      Set<JDOPersistenceManager> objectsAfterDroppedConnection = new HashSet<>(getJDOPersistenceManagerCache());
      objectsAfterDroppedConnection.removeAll(objectsAfterClose);

      assertEquals("new objects left in the cache (after dropping connection)", 0,
          objectsAfterDroppedConnection.size());

    }, 500, 30000);

    nonClosingClient.close(); //let's close after unit test ran successfully
  }

  public static Set<JDOPersistenceManager> getJDOPersistenceManagerCache() {
    JDOPersistenceManagerFactory jdoPmf;
    Set<JDOPersistenceManager> pmCacheObj = null;
    Field pmCache;
    try {
      Field pmf = PersistenceManagerProvider.class.getDeclaredField("pmf");
      if (pmf != null) {
        pmf.setAccessible(true);
        jdoPmf = (JDOPersistenceManagerFactory) pmf.get(null);
        pmCache = JDOPersistenceManagerFactory.class.getDeclaredField("pmCache");
        if (pmCache != null) {
          pmCache.setAccessible(true);
          pmCacheObj = (Set<JDOPersistenceManager>) pmCache.get(jdoPmf);
          return pmCacheObj;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return pmCacheObj;
  }

  private HiveMetaHookLoader getHookLoader() {
    HiveMetaHookLoader hookLoader = new HiveMetaHookLoader() {
      @Override
      public HiveMetaHook getHook(
          org.apache.hadoop.hive.metastore.api.Table tbl)
          throws MetaException {
        return null;
      }
    };
    return hookLoader;
  }

  @Test
  public void testValidateTableCols() throws Throwable {

    try {
      String dbName = "compdb";
      String tblName = "comptbl";

      client.dropTable(dbName, tblName);
      silentDropDatabase(dbName);
      new DatabaseBuilder()
          .setName(dbName)
          .setDescription("Validate Table Columns test")
          .create(client, conf);

      Table tbl = new TableBuilder()
          .setDbName(dbName)
          .setTableName(tblName)
          .addCol("name", ColumnType.STRING_TYPE_NAME)
          .addCol("income", ColumnType.INT_TYPE_NAME)
          .create(client, conf);

      if (isThriftClient) {
        tbl = client.getTable(dbName, tblName);
      }

      List<String> expectedCols = Lists.newArrayList();
      expectedCols.add("name");
      ObjectStore objStore = new ObjectStore();
      try {
        objStore.validateTableCols(tbl, expectedCols);
      } catch (MetaException ex) {
        throw new RuntimeException(ex);
      }

      expectedCols.add("doesntExist");
      boolean exceptionFound = false;
      try {
        objStore.validateTableCols(tbl, expectedCols);
      } catch (MetaException ex) {
        assertEquals(ex.getMessage(),
            "Column doesntExist doesn't exist in table comptbl in database compdb");
        exceptionFound = true;
      }
      assertTrue(exceptionFound);

    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testValidateTableCols() failed.");
      throw e;
    }
  }

  @Test
  public void testGetMetastoreUuid() throws Throwable {
    String uuid = client.getMetastoreDbUuid();
    assertNotNull(uuid);
  }

  @Test
  public void testGetUUIDInParallel() throws Exception {
    int numThreads = 5;
    int parallelCalls = 10;
    int numAPICallsPerThread = 10;
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
    List<Future<List<String>>> futures = new ArrayList<>();
    for (int n = 0; n < parallelCalls; n++) {
      futures.add(executorService.submit(new Callable<List<String>>() {
        @Override
        public List<String> call() throws Exception {
          HiveMetaStoreClient testClient = new HiveMetaStoreClient(conf);
          List<String> uuids = new ArrayList<>(10);
          for (int i = 0; i < numAPICallsPerThread; i++) {
            String uuid = testClient.getMetastoreDbUuid();
            uuids.add(uuid);
          }
          return uuids;
        }
      }));
    }

    String firstUUID = null;
    List<String> allUuids = new ArrayList<>();
    for (Future<List<String>> future : futures) {
      for (String uuid : future.get()) {
        if (firstUUID == null) {
          firstUUID = uuid;
        } else {
          assertEquals(firstUUID.toLowerCase(), uuid.toLowerCase());
        }
        allUuids.add(uuid);
      }
    }
    int size = allUuids.size();
    assertEquals(numAPICallsPerThread * parallelCalls, size);
  }

  @Test
  public void testVersion() throws TException {
    assertEquals(MetastoreVersionInfo.getVersion(), client.getServerVersion());
  }

  /**
   * While altering partition(s), verify DO NOT calculate partition statistics if
   * <ol>
   *   <li>table property DO_NOT_UPDATE_STATS is true</li>
   *   <li>STATS_AUTO_GATHER is false</li>
   *   <li>Is View</li>
   * </ol>
   */
  @Test
  public void testUpdatePartitionStat_doesNotUpdateStats() throws Exception {
    final String DB_NAME = "db1";
    final String TABLE_NAME = "tbl1";
    Table tbl = new TableBuilder()
        .setDbName(DB_NAME)
        .setTableName(TABLE_NAME)
        .addCol("id", "int")
        .addTableParam(StatsSetupConst.DO_NOT_UPDATE_STATS, "true")
        .build(null);
    List<String> vals = new ArrayList<>(2);
    vals.add("col1");
    vals.add("col2");
    Partition part = new Partition();
    part.setDbName(DB_NAME);
    part.setTableName(TABLE_NAME);
    part.setValues(vals);
    part.setParameters(new HashMap<>());
    part.setSd(tbl.getSd().deepCopy());
    part.getSd().setSerdeInfo(tbl.getSd().getSerdeInfo());
    part.getSd().setLocation(tbl.getSd().getLocation() + "/partCol=1");
    Warehouse wh = mock(Warehouse.class);
    //Execute initializeAddedPartition() and it should not trigger updatePartitionStatsFast() as DO_NOT_UPDATE_STATS is true
    HMSHandler hms = new HMSHandler("", conf);
    Method m = hms.getClass().getDeclaredMethod("initializeAddedPartition", Table.class, Partition.class,
            boolean.class, EnvironmentContext.class);
    m.setAccessible(true);
    //Invoke initializeAddedPartition();
    m.invoke(hms, tbl, part, false, null);
    verify(wh, never()).getFileStatusesForLocation(part.getSd().getLocation());

    //Remove tbl's DO_NOT_UPDATE_STATS & set STATS_AUTO_GATHER = false
    tbl.unsetParameters();
    MetastoreConf.setBoolVar(conf, ConfVars.STATS_AUTO_GATHER, false);
    m.invoke(hms, tbl, part, false, null);
    verify(wh, never()).getFileStatusesForLocation(part.getSd().getLocation());

    //Set STATS_AUTO_GATHER = true and set tbl as a VIRTUAL_VIEW
    MetastoreConf.setBoolVar(conf, ConfVars.STATS_AUTO_GATHER, true);
    tbl.setTableType("VIRTUAL_VIEW");
    m.invoke(hms, tbl, part, false, null);
    verify(wh, never()).getFileStatusesForLocation(part.getSd().getLocation());
  }


  public void testAlterTableRenameBucketedColumnPositive() throws Exception {
    String dbName = "alterTblDb";
    String tblName = "altertbl";

    client.dropTable(dbName, tblName);
    silentDropDatabase(dbName);

    new DatabaseBuilder().setName(dbName).create(client, conf);

    ArrayList<FieldSchema> origCols = new ArrayList<>(2);
    origCols.add(new FieldSchema("originalColName", ColumnType.STRING_TYPE_NAME, ""));
    origCols.add(new FieldSchema("income", ColumnType.INT_TYPE_NAME, ""));
    Table origTbl = new TableBuilder().setDbName(dbName).setTableName(tblName).setCols(origCols)
        .setBucketCols(Lists.newArrayList("originalColName")).build(conf);
    client.createTable(origTbl);

    // Rename bucketed column positive case
    ArrayList<FieldSchema> colsUpdated = new ArrayList<>(origCols);
    colsUpdated.set(0, new FieldSchema("updatedColName1", ColumnType.STRING_TYPE_NAME, ""));
    Table tblUpdated = client.getTable(dbName, tblName);
    tblUpdated.getSd().setCols(colsUpdated);
    tblUpdated.getSd().getBucketCols().set(0, colsUpdated.get(0).getName());
    client.alter_table(dbName, tblName, tblUpdated);

    Table resultTbl = client.getTable(dbName, tblUpdated.getTableName());
    assertEquals("Num bucketed columns is not 1 ", 1, resultTbl.getSd().getBucketCols().size());
    assertEquals("Bucketed column names incorrect", colsUpdated.get(0).getName(),
        resultTbl.getSd().getBucketCols().get(0));

    silentDropDatabase(dbName);
  }

  @Test(expected = InvalidOperationException.class)
  public void testAlterTableRenameBucketedColumnNegative() throws Exception {
    String dbName = "alterTblDb";
    String tblName = "altertbl";

    client.dropTable(dbName, tblName);
    silentDropDatabase(dbName);

    new DatabaseBuilder().setName(dbName).create(client, conf);

    ArrayList<FieldSchema> origCols = new ArrayList<>(2);
    origCols.add(new FieldSchema("originalColName", ColumnType.STRING_TYPE_NAME, ""));
    origCols.add(new FieldSchema("income", ColumnType.INT_TYPE_NAME, ""));
    Table origTbl = new TableBuilder().setDbName(dbName).setTableName(tblName).setCols(origCols)
        .setBucketCols(Lists.newArrayList("originalColName")).build(conf);
    client.createTable(origTbl);

    // Rename bucketed column negative case
    ArrayList<FieldSchema> colsUpdated = new ArrayList<>(origCols);
    colsUpdated.set(0, new FieldSchema("updatedColName1", ColumnType.STRING_TYPE_NAME, ""));
    Table tblUpdated = client.getTable(dbName, tblName);
    tblUpdated.getSd().setCols(colsUpdated);
    client.alter_table(dbName, tblName, tblUpdated);

    silentDropDatabase(dbName);
  }

  @Test(expected = MetaException.class)
  public void testAlterTableCascadeExceedsPartitionLimits() throws Throwable {
    String dbName = "alterTblDb";
    String tblName = "altertbl";
    String ds = "2025-05-21 23:47:12";

    cleanUp(dbName, tblName, null);

    // Create too many partitions, just enough to validate over limit requests
    List<List<String>> values = new ArrayList<>();
    for (int i = 0; i < DEFAULT_LIMIT_PARTITION_REQUEST + 1; i++) {
      values.add(makeVals(ds, Integer.toString(i)));
    }

    createMultiPartitionTableSchema(dbName, tblName, null, values);

    Table tbl = client.getTable(dbName, tblName);
    List<FieldSchema> cols = tbl.getSd().getCols();
    cols.add(new FieldSchema("new_col", ColumnType.STRING_TYPE_NAME, ""));
    tbl.getSd().setCols(cols);
    //add new column with cascade option
    client.alter_table(dbName, tblName, tbl, true);
  }

  @Test
  public void testDataConnector() throws Throwable {
    final String connector_name1 = "test_connector1";
    final String connector_name2 = "test_connector2";
    final String mysql_type = "mysql";
    final String mysql_url = "jdbc:mysql://nightly1.apache.org:3306/hive1";
    final String postgres_type = "postgres";
    final String postgres_url = "jdbc:postgresql://localhost:5432";

    try {
      DataConnector connector = new DataConnector(connector_name1, mysql_type, mysql_url);
      Map<String, String> params = new HashMap<>();
      params.put(AbstractJDBCConnectorProvider.JDBC_USERNAME, "hive");
      params.put(AbstractJDBCConnectorProvider.JDBC_PASSWORD, "hive");
      connector.setParameters(params);
      client.createDataConnector(connector);

      DataConnector dConn = client.getDataConnector(connector_name1);
      assertNotNull(dConn);
      assertEquals("name of returned data connector is different from that of inserted connector", connector_name1,
          dConn.getName());
      assertEquals("type of data connector returned is different from the type inserted", mysql_type, dConn.getType());
      assertEquals("url of the data connector returned is different from the url inserted", mysql_url, dConn.getUrl());
      // assertEquals(SecurityUtils.getUser(), dConn.getOwnerName());
      assertEquals(PrincipalType.USER, dConn.getOwnerType());
      assertNotEquals("Size of data connector parameters not as expected", 0, dConn.getParametersSize());

      try {
        client.createDataConnector(connector);
        fail("Creating duplicate connector should fail");
      } catch (Exception e) { /* as expected */ }

      connector = new DataConnector(connector_name2, postgres_type, postgres_url);
      params = new HashMap<>();
      params.put(AbstractJDBCConnectorProvider.JDBC_USERNAME, "hive");
      params.put(AbstractJDBCConnectorProvider.JDBC_PASSWORD, "hive");
      connector.setParameters(params);
      client.createDataConnector(connector);

      dConn = client.getDataConnector(connector_name2);
      assertEquals("name of returned data connector is different from that of inserted connector", connector_name2,
          dConn.getName());
      assertEquals("type of data connector returned is different from the type inserted", postgres_type, dConn.getType());
      assertEquals("url of the data connector returned is different from the url inserted", postgres_url, dConn.getUrl());

      List<String> connectors = client.getAllDataConnectorNames();
      assertEquals("Number of dataconnectors returned is not as expected", 2, connectors.size());

      DataConnector connector1 = new DataConnector(connector);
      connector1.setUrl(mysql_url);
      client.alterDataConnector(connector.getName(), connector1);

      dConn = client.getDataConnector(connector.getName());
      assertEquals("url of the data connector returned is different from the url inserted", mysql_url, dConn.getUrl());

      // alter data connector parameters
      params.put(AbstractJDBCConnectorProvider.JDBC_NUM_PARTITIONS, "5");
      connector1.setParameters(params);
      client.alterDataConnector(connector.getName(), connector1);

      dConn = client.getDataConnector(connector.getName());
      assertEquals("Size of data connector parameters not as expected", 3, dConn.getParametersSize());

      // alter data connector parameters
      connector1.setOwnerName("hiveadmin");
      connector1.setOwnerType(PrincipalType.ROLE);
      client.alterDataConnector(connector.getName(), connector1);

      dConn = client.getDataConnector(connector.getName());
      assertEquals("Data connector owner name not as expected", "hiveadmin", dConn.getOwnerName());
      assertEquals("Data connector owner type not as expected", PrincipalType.ROLE, dConn.getOwnerType());

      client.dropDataConnector(connector_name1, false, false);
      connectors = client.getAllDataConnectorNames();
      assertEquals("Number of dataconnectors returned is not as expected", 1, connectors.size());

      client.dropDataConnector(connector_name2, false, false);
      connectors = client.getAllDataConnectorNames();
      assertEquals("Number of dataconnectors returned is not as expected", 0, connectors.size());
    } catch (Throwable e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testDataConnector() failed.");
      throw e;
    }
  }

  @Test
  public void testRemoteDatabase() throws Throwable {
    final String connector_name1 = "test_connector1";
    final String mysql_type = "mysql";
    final String mysql_url = "jdbc:mysql://nightly1.apache.org:3306/hive1";
    final String db_name = "mysql_remote";
    final String db2 = "mysql_dup";

    try {
      DataConnector connector = new DataConnector(connector_name1, mysql_type, mysql_url);
      Map<String, String> params = new HashMap<>();
      params.put(AbstractJDBCConnectorProvider.JDBC_USERNAME, "hive");
      params.put(AbstractJDBCConnectorProvider.JDBC_PASSWORD, "hive");
      connector.setParameters(params);
      client.createDataConnector(connector);

      DataConnector dConn = client.getDataConnector(connector_name1);
      new DatabaseBuilder().setName(db_name).setType(DatabaseType.REMOTE).setConnectorName(connector_name1)
          .setRemoteDBName(db_name).create(client, conf);

      Database db = client.getDatabase(db_name);
      assertNotNull(db);
      assertEquals(db.getType(), DatabaseType.REMOTE);
      assertEquals(db.getConnector_name(), connector_name1);
      assertEquals(db.getRemote_dbname(), db_name);

      // new db in hive pointing to same remote db.
      new DatabaseBuilder().setName(db2).setType(DatabaseType.REMOTE).setConnectorName(connector_name1)
          .setRemoteDBName(db_name).create(client, conf);

      db = client.getDatabase(db2);
      assertNotNull(db);
      assertEquals(db.getType(), DatabaseType.REMOTE);
      assertEquals(db.getConnector_name(), connector_name1);
      assertEquals(db.getRemote_dbname(), db_name);

      client.dropDataConnector(connector_name1, false, false);
      client.dropDatabase(db_name);
      client.dropDatabase(db2);
    } catch (Throwable e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err.println("testRemoteDatabase() failed.");
      throw e;
    }
  }

  @Test(expected = NoSuchObjectException.class)
  public void testDropDataConnectorIfNotExistsFalse() throws Exception {
    // No such data connector, throw NoSuchObjectException
    client.dropDataConnector("no_such_data_connector", false, false);
  }

  @Test
  public void testDropDataConnectorIfNotExistsTrue() throws Exception {
    // No such data connector, ignore NoSuchObjectException
    client.dropDataConnector("no_such_data_connector", true, false);
  }
}
