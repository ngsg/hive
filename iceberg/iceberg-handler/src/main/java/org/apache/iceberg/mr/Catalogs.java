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

import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.parquet.Strings;

/**
 * Class for catalog resolution and accessing the common functions for {@link Catalog} API.
 *
 * <p>If the catalog name is provided, get the catalog type from iceberg.catalog.<code>catalogName
 * </code>.type config.
 *
 * <p>In case the catalog name is {@link #ICEBERG_HADOOP_TABLE_NAME location_based_table}, type is
 * ignored and tables will be loaded using {@link HadoopTables}.
 *
 * <p>In case the value of catalog type is null, iceberg.catalog.<code>catalogName</code>
 * .catalog-impl config is used to determine the catalog implementation class.
 *
 * <p>If catalog name is null, get the catalog type from {@link CatalogUtil#ICEBERG_CATALOG_TYPE
 * catalog type} config:
 *
 * <ul>
 *   <li>hive: HiveCatalog</li>
 *   <li>location: HadoopTables</li>
 *   <li>hadoop: HadoopCatalog</li>
 * </ul>
 */
public final class Catalogs {

  public static final String ICEBERG_DEFAULT_CATALOG_NAME = "default_iceberg";
  public static final String ICEBERG_HADOOP_TABLE_NAME = "location_based_table";

  public static final String NAME = "name";
  public static final String LOCATION = "location";
  public static final String SNAPSHOT_REF = "snapshot_ref";

  private static final String NO_CATALOG_TYPE = "no catalog";
  private static final Set<String> PROPERTIES_TO_REMOVE =
      ImmutableSet.of(InputFormatConfig.TABLE_SCHEMA, InputFormatConfig.PARTITION_SPEC, LOCATION, NAME,
              InputFormatConfig.CATALOG_NAME);

  private Catalogs() {
  }

  /**
   * Load an Iceberg table using the catalog and table identifier (or table path) specified by the configuration.
   * @param conf a Hadoop conf
   * @return an Iceberg table
   */
  public static Table loadTable(Configuration conf) {
    return loadTable(conf, conf.get(InputFormatConfig.TABLE_IDENTIFIER), conf.get(InputFormatConfig.TABLE_LOCATION),
            conf.get(InputFormatConfig.CATALOG_NAME));
  }

  /**
   * Load an Iceberg table using the catalog specified by the configuration.
   * <p>
   * The table identifier ({@link Catalogs#NAME}) and the catalog name ({@link InputFormatConfig#CATALOG_NAME}),
   * or table path ({@link Catalogs#LOCATION}) should be specified by the controlling properties.
   * <p>
   * Used by HiveIcebergSerDe and HiveIcebergStorageHandler.
   * @param conf a Hadoop configuration
   * @param props the controlling properties
   * @return an Iceberg table
   */
  public static Table loadTable(Configuration conf, Properties props) {
    return loadTable(conf, props.getProperty(NAME), props.getProperty(LOCATION),
            props.getProperty(InputFormatConfig.CATALOG_NAME));
  }

  private static Table loadTable(Configuration conf, String tableIdentifier, String tableLocation,
                                 String catalogName) {
    Optional<Catalog> catalog = loadCatalog(conf, catalogName);

    if (catalog.isPresent()) {
      Preconditions.checkArgument(tableIdentifier != null, "Table identifier not set");
      return catalog.get().loadTable(TableIdentifier.parse(tableIdentifier));
    }

    Preconditions.checkArgument(tableLocation != null, "Table location not set");
    return new HadoopTables(conf).load(tableLocation);
  }

  /**
   * Creates an Iceberg table using the catalog specified by the configuration.
   * <p>
   * The properties should contain the following values:
   * <ul>
   * <li>Table identifier ({@link Catalogs#NAME}) or table path ({@link Catalogs#LOCATION}) is required
   * <li>Table schema ({@link InputFormatConfig#TABLE_SCHEMA}) is required
   * <li>Partition specification ({@link InputFormatConfig#PARTITION_SPEC}) is optional. Table will be unpartitioned if
   *  not provided
   * </ul><p>
   * Other properties will be handled over to the Table creation. The controlling properties above will not be
   * propagated.
   * @param conf a Hadoop conf
   * @param props the controlling properties
   * @return the created Iceberg table
   */
  public static Table createTable(Configuration conf, Properties props) {
    Schema schema = schema(props);
    PartitionSpec spec = spec(props, schema);
    String location = props.getProperty(LOCATION);
    String catalogName = props.getProperty(InputFormatConfig.CATALOG_NAME);

    Map<String, String> map = filterIcebergTableProperties(props);

    Optional<Catalog> catalog = loadCatalog(conf, catalogName);
    SortOrder sortOrder = getSortOrder(props, schema);
    if (catalog.isPresent()) {
      String name = props.getProperty(NAME);
      Preconditions.checkNotNull(name, "Table identifier not set");
      return catalog.get().buildTable(TableIdentifier.parse(name), schema).withPartitionSpec(spec)
          .withLocation(location).withProperties(map).withSortOrder(sortOrder).create();
    }

    Preconditions.checkNotNull(location, "Table location not set");
    return new HadoopTables(conf).create(schema, spec, sortOrder, map, location);
  }

  private static SortOrder getSortOrder(Properties props, Schema schema) {
    String sortOrderJsonString = props.getProperty(TableProperties.DEFAULT_SORT_ORDER);
    return Strings.isNullOrEmpty(sortOrderJsonString) ?
        SortOrder.unsorted() : SortOrderParser.fromJson(schema, sortOrderJsonString);
  }

  /**
   * Drops an Iceberg table using the catalog specified by the configuration.
   * <p>
   * The table identifier ({@link Catalogs#NAME}) or table path ({@link Catalogs#LOCATION}) should be specified by
   * the controlling properties.
   * @param conf a Hadoop conf
   * @param props the controlling properties
   * @return the created Iceberg table
   */
  public static boolean dropTable(Configuration conf, Properties props) {
    String location = props.getProperty(LOCATION);
    String catalogName = props.getProperty(InputFormatConfig.CATALOG_NAME);

    Optional<Catalog> catalog = loadCatalog(conf, catalogName);

    if (catalog.isPresent()) {
      String name = props.getProperty(NAME);
      Preconditions.checkNotNull(name, "Table identifier not set");
      return catalog.get().dropTable(TableIdentifier.parse(name));
    }

    Preconditions.checkNotNull(location, "Table location not set");
    return new HadoopTables(conf).dropTable(location);
  }

  /**
   * Returns true if HiveCatalog is used
   * @param conf a Hadoop conf
   * @param props the controlling properties
   * @return true if the Catalog is HiveCatalog
   */
  public static boolean hiveCatalog(Configuration conf, Properties props) {
    String catalogName = props.getProperty(InputFormatConfig.CATALOG_NAME);
    String catalogType = getCatalogType(conf, catalogName);
    if (catalogType != null) {
      return CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE.equalsIgnoreCase(catalogType);
    }
    catalogType = getCatalogType(conf, ICEBERG_DEFAULT_CATALOG_NAME);
    if (catalogType != null) {
      return CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE.equalsIgnoreCase(catalogType);
    }
    return getCatalogProperties(conf, catalogName).get(CatalogProperties.CATALOG_IMPL) == null;
  }

  public static boolean hadoopCatalog(Configuration conf, Properties props) {
    String catalogName = props.getProperty(InputFormatConfig.CATALOG_NAME);
    String catalogType = getCatalogType(conf, catalogName);
    if (catalogType != null) {
      return CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP.equalsIgnoreCase(catalogType);
    }
    catalogType = getCatalogType(conf, ICEBERG_DEFAULT_CATALOG_NAME);
    if (catalogType != null) {
      return CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP.equalsIgnoreCase(catalogType);
    }
    return CatalogUtil.ICEBERG_CATALOG_HADOOP.equals(
        getCatalogProperties(conf, catalogName).get(CatalogProperties.CATALOG_IMPL));
  }

  /**
   * Register a table with the configured catalog if it does not exist.
   * @param conf a Hadoop conf
   * @param props the controlling properties
   * @param metadataLocation the location of a metadata file
   * @return the created Iceberg table
   */
  public static Table registerTable(Configuration conf, Properties props, String metadataLocation) {
    Schema schema = schema(props);
    PartitionSpec spec = spec(props, schema);
    Map<String, String> map = filterIcebergTableProperties(props);
    String location = props.getProperty(LOCATION);
    String catalogName = props.getProperty(InputFormatConfig.CATALOG_NAME);

    Optional<Catalog> catalog = loadCatalog(conf, catalogName);
    if (catalog.isPresent()) {
      String name = props.getProperty(NAME);
      Preconditions.checkNotNull(name, "Table identifier not set");
      return catalog.get().registerTable(TableIdentifier.parse(name), metadataLocation);
    }
    Preconditions.checkNotNull(location, "Table location not set");
    SortOrder sortOrder = getSortOrder(props, schema);
    return new HadoopTables(conf).create(schema, spec, sortOrder, map, location);
  }

  public static void renameTable(Configuration conf, Properties props, TableIdentifier to) {
    String catalogName = props.getProperty(InputFormatConfig.CATALOG_NAME);

    Optional<Catalog> catalog = loadCatalog(conf, catalogName);
    if (catalog.isPresent()) {
      String name = props.getProperty(NAME);
      Preconditions.checkNotNull(name, "Table identifier not set");
      catalog.get().renameTable(TableIdentifier.parse(name), to);
    } else {
      throw new RuntimeException("Rename from " + props.getProperty(NAME) + " to " + to + " failed");
    }
  }

  static Optional<Catalog> loadCatalog(Configuration conf, String catalogName) {
    String catalogType = getCatalogType(conf, catalogName);
    if (NO_CATALOG_TYPE.equalsIgnoreCase(catalogType)) {
      return Optional.empty();
    } else {
      String name = catalogName == null ? ICEBERG_DEFAULT_CATALOG_NAME : catalogName;
      return Optional.of(CatalogUtil.buildIcebergCatalog(name,
              getCatalogProperties(conf, name), conf));
    }
  }

  /**
   * Collect all the catalog specific configuration from the global hive configuration.
   * @param conf a Hadoop configuration
   * @param catalogName name of the catalog
   * @return complete map of catalog properties
   */
  private static Map<String, String> getCatalogProperties(Configuration conf, String catalogName) {
    Map<String, String> catalogProperties = Maps.newHashMap();
    String keyPrefix = InputFormatConfig.CATALOG_CONFIG_PREFIX + catalogName;
    conf.forEach(config -> {
      if (config.getKey().startsWith(InputFormatConfig.CATALOG_DEFAULT_CONFIG_PREFIX)) {
        catalogProperties.putIfAbsent(
                config.getKey().substring(InputFormatConfig.CATALOG_DEFAULT_CONFIG_PREFIX.length()),
                config.getValue());
      } else if (config.getKey().startsWith(keyPrefix)) {
        catalogProperties.put(
                config.getKey().substring(keyPrefix.length() + 1),
                config.getValue());
      }
    });

    return catalogProperties;
  }

  /**
   * Return the catalog type based on the catalog name.
   * <p>
   * See {@link Catalogs} documentation for catalog type resolution strategy.
   *
   * @param conf global hive configuration
   * @param catalogName name of the catalog
   * @return type of the catalog, can be null
   */
  private static String getCatalogType(Configuration conf, String catalogName) {
    if (catalogName != null) {
      String catalogType = conf.get(InputFormatConfig.catalogPropertyConfigKey(
          catalogName, CatalogUtil.ICEBERG_CATALOG_TYPE));
      if (catalogName.equals(ICEBERG_HADOOP_TABLE_NAME)) {
        return NO_CATALOG_TYPE;
      } else {
        return catalogType;
      }
    } else {
      String catalogType = conf.get(CatalogUtil.ICEBERG_CATALOG_TYPE);
      if (catalogType != null && catalogType.equals(LOCATION)) {
        return NO_CATALOG_TYPE;
      } else {
        return catalogType;
      }
    }
  }

  /**
   * Parse the table schema from the properties
   * @param props the controlling properties
   * @return schema instance
   */
  private static Schema schema(Properties props) {
    String schemaString = props.getProperty(InputFormatConfig.TABLE_SCHEMA);
    Preconditions.checkNotNull(schemaString, "Table schema not set");
    return SchemaParser.fromJson(schemaString);
  }

  /**
   * Get the partition spec from the properties
   * @param props the controlling properties
   * @param schema instance of the iceberg schema
   * @return  instance of the partition spec
   */
  private static PartitionSpec spec(Properties props, Schema schema) {
    String specString = props.getProperty(InputFormatConfig.PARTITION_SPEC);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    if (specString != null) {
      spec = PartitionSpecParser.fromJson(schema, specString);
    }
    return spec;
  }

  /**
   * Create the iceberg table properties without the {@link Catalogs#PROPERTIES_TO_REMOVE}
   * @param props the controlling properties
   * @return map of iceberg table properties
   */
  private static Map<String, String> filterIcebergTableProperties(Properties props) {
    Map<String, String> map = Maps.newHashMapWithExpectedSize(props.size());
    for (Object key : props.keySet()) {
      if (!PROPERTIES_TO_REMOVE.contains(key)) {
        map.put(key.toString(), props.get(key).toString());
      }
    }
    return map;
  }
}
