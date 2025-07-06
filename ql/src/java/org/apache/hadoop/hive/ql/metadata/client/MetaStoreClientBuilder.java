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

package org.apache.hadoop.hive.ql.metadata.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.client.HookEnabledMetaStoreClient;
import org.apache.hadoop.hive.metastore.client.SynchronizedMetaStoreClient;
import org.apache.hadoop.hive.metastore.client.ThriftHiveMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.HiveMetaStoreClientWithLocalCache;
import org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClient;

import java.util.concurrent.ConcurrentHashMap;

public class MetaStoreClientBuilder {
  private final Configuration conf;

  private HiveMetaHookLoader hookLoader = null;
  private ConcurrentHashMap<String, Long> metaCallTimeMap = null;
  private IMetaStoreClient baseClient = null;

  private boolean isRetryEnabled = false;
  private final boolean[] enabledFeatures = {false, false, false, false};

  private enum Feature {
    LOCAL_CACHE, SESSION, HOOK, SYNCHRONIZED
  }

  public MetaStoreClientBuilder(Configuration conf) {
    this.conf = conf;
  }

  public MetaStoreClientBuilder newThriftClient(boolean allowEmbedded) throws MetaException {
    this.baseClient = ThriftHiveMetaStoreClient.newClient(conf, allowEmbedded);
    return this;
  }

  public MetaStoreClientBuilder withLocalCache() {
    this.enabledFeatures[Feature.LOCAL_CACHE.ordinal()] = true;
    return this;
  }

  public MetaStoreClientBuilder withSessionWrapper() {
    this.enabledFeatures[Feature.SESSION.ordinal()] = true;
    return this;
  }

  public MetaStoreClientBuilder withHooks(HiveMetaHookLoader hookLoader) {
    this.enabledFeatures[Feature.HOOK.ordinal()] = true;
    this.hookLoader = hookLoader;
    return this;
  }

  public MetaStoreClientBuilder withSynchronizedWrapper() {
    this.enabledFeatures[Feature.SYNCHRONIZED.ordinal()] = true;
    return this;
  }

  public MetaStoreClientBuilder withRetry(ConcurrentHashMap<String, Long> metaCallTimeMap) {
    this.isRetryEnabled = true;
    this.metaCallTimeMap = metaCallTimeMap;
    return this;
  }

  public IMetaStoreClient build() throws MetaException {
    IMetaStoreClient client = baseClient;

    if (enabledFeatures[Feature.LOCAL_CACHE.ordinal()]) {
      if (isRetryEnabled && isLastWrapper(Feature.LOCAL_CACHE)) {
        client = RetryingMetaStoreClient.getProxy(
            conf,
            new Class[] {Configuration.class, IMetaStoreClient.class},
            new Object[] {conf, client},
            metaCallTimeMap,
            HiveMetaStoreClientWithLocalCache.class.getName()
        );
      } else {
        client = HiveMetaStoreClientWithLocalCache.newClient(conf, client);
      }
    }

    if (enabledFeatures[Feature.SESSION.ordinal()]) {
      if (isRetryEnabled && isLastWrapper(Feature.SESSION)) {
        client = RetryingMetaStoreClient.getProxy(
            conf,
            new Class[] {Configuration.class, IMetaStoreClient.class},
            new Object[] {conf, client},
            metaCallTimeMap,
            SessionHiveMetaStoreClient.class.getName()
        );
      } else {
        client = SessionHiveMetaStoreClient.newClient(conf, client);
      }
    }

    if (enabledFeatures[Feature.HOOK.ordinal()]) {
      if (isRetryEnabled && isLastWrapper(Feature.HOOK)) {
        client = RetryingMetaStoreClient.getProxy(
            conf,
            new Class[] {Configuration.class, HiveMetaHookLoader.class, IMetaStoreClient.class},
            new Object[] {conf, hookLoader, client},
            metaCallTimeMap,
            HookEnabledMetaStoreClient.class.getName()
        );
      } else {
        client = HookEnabledMetaStoreClient.newClient(conf, hookLoader, client);
      }
    }

    if (enabledFeatures[Feature.SYNCHRONIZED.ordinal()]) {
      if (isRetryEnabled && isLastWrapper(Feature.SYNCHRONIZED)) {
        client = RetryingMetaStoreClient.getProxy(
            conf,
            new Class[] {Configuration.class, IMetaStoreClient.class},
            new Object[] {conf, client},
            metaCallTimeMap,
            SynchronizedMetaStoreClient.class.getName()
        );
      } else {
        client = SynchronizedMetaStoreClient.newClient(conf, client);
      }
    }

    return client;
  }

  private boolean isLastWrapper(Feature feature) {
    boolean foundOuterWrapper = false;
    for (int i = feature.ordinal() + 1; i < enabledFeatures.length; i++) {
      foundOuterWrapper = foundOuterWrapper | enabledFeatures[i];
    }
    return !foundOuterWrapper;
  }
}