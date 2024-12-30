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
package org.apache.hadoop.hbase.compactionserver;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.StartTestingClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.TestMobCompactionWithDefaults;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestMobCompactionOffload extends TestMobCompactionWithDefaults {
  private static HCompactionServer COMPACTION_SERVER;
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMobCompactionOffload.class);

  public TestMobCompactionOffload(Boolean useFileBasedSFT) {
    super(useFileBasedSFT);
  }

  public void htuStartUp() throws Exception {
    HTU = new HBaseTestingUtil();
    conf = HTU.getConfiguration();
    conf.setInt("hfile.format.version", 3);
    // Disable automatic MOB compaction
    conf.setLong(MobConstants.MOB_COMPACTION_CHORE_PERIOD, 0);
    // Disable automatic MOB file cleaner chore
    conf.setLong(MobConstants.MOB_CLEANER_PERIOD, 0);
    // Set minimum age to archive to 10 sec
    conf.setLong(MobConstants.MIN_AGE_TO_ARCHIVE_KEY, minAgeToArchive);
    // Set compacted file discharger interval to a half minAgeToArchive
    conf.setLong("hbase.hfile.compaction.discharger.interval", minAgeToArchive / 2);
    conf.setBoolean("hbase.regionserver.compaction.enabled", false);
    if (useFileBasedSFT) {
      conf.set(StoreFileTrackerFactory.TRACKER_IMPL,
        "org.apache.hadoop.hbase.regionserver.storefiletracker.FileBasedStoreFileTracker");
    }

    HTU.startMiniCluster(StartTestingClusterOption.builder().numCompactionServers(1).build());
    HTU.getAdmin().switchCompactionOffload(true);
    HTU.getMiniHBaseCluster().waitForActiveAndReadyMaster();
    COMPACTION_SERVER =
      HTU.getMiniHBaseCluster().getCompactionServerThreads().get(0).getCompactionServer();

    additonalConfigSetup();
    HTU.startMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    htuStart();
    admin = HTU.getAdmin();
    familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(fam).setMobEnabled(true)
      .setMobThreshold(mobLen).setMaxVersions(1).build();
    tableDescriptor = HTU.createModifyableTableDescriptor(test.getMethodName())
      .setColumnFamily(familyDescriptor).setCompactionOffloadEnabled(true).build();
    RegionSplitter.UniformSplit splitAlgo = new RegionSplitter.UniformSplit();
    byte[][] splitKeys = splitAlgo.split(numRegions);
    table = HTU.createTable(tableDescriptor, splitKeys).getName();
    COMPACTION_SERVER.requestCount.reset();
  }

  @After
  public void tearDown() throws Exception {
    // ensure do compaction on compaction server
    HTU.waitFor(6000, () -> COMPACTION_SERVER.requestCount.sum() > 0);
    admin.disableTable(tableDescriptor.getTableName());
    admin.deleteTable(tableDescriptor.getTableName());
  }

  @Override
  protected void waitUntilCompactionIsComplete(TableName table) {
    while (COMPACTION_SERVER.compactionThreadManager.getRunningCompactionTasks().size() > 0) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
