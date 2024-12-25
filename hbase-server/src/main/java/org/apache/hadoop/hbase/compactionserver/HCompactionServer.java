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

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import javax.servlet.http.HttpServlet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseServerBase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.YouAreDeadException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.http.InfoServer;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.namequeues.NamedQueueRecorder;
import org.apache.hadoop.hbase.security.SecurityConstants;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.Sleeper;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.ZKAuthentication;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.CompactionServerStatusProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.CompactionServerStatusProtos.CompactionServerStatusService;

@InterfaceAudience.Private
public class HCompactionServer extends HBaseServerBase {

  /** compaction server process name */
  public static final String COMPACTIONSERVER = "compactionserver";
  private static final Logger LOG = LoggerFactory.getLogger(HCompactionServer.class);

  @Override
  protected String getProcessName() {
    return COMPACTIONSERVER;
  }

  @Override
  public CoordinatedStateManager getCoordinatedStateManager() {
    return null;
  }

  @Override
  protected CoprocessorHost<?, ?> getCoprocessorHost() {
    return null;
  }

  @Override
  protected boolean canCreateBaseZNode() {
    return false;
  }

  @Override
  public ChoreService getChoreService() {
    return null;
  }

  protected final CSRpcServices rpcServices;

  // Stub to do compaction server status calls against the master.
  private volatile CompactionServerStatusService.BlockingInterface cssStub;

  /**
   * Get the current master from ZooKeeper and open the RPC connection to it. To get a fresh
   * connection, the current cssStub must be null. Method will block until a master is available.
   * You can break from this block by requesting the server stop.
   * @return master + port, or null if server has been stopped
   */
  private synchronized void createCompactionServerStatusStub() {
    // Create CS stub without refreshing the master node from ZK, use cached data
    if (cssStub == null) {
      cssStub = (CompactionServerStatusService.BlockingInterface) createMasterStub(
        CompactionServerStatusService.class, false);
    }
  }

  protected CSRpcServices createRpcServices() throws IOException {
    return new CSRpcServices(this);
  }

  @Override
  protected String getUseThisHostnameInstead(Configuration conf) throws IOException {
    return "";
  }

  public HCompactionServer(final Configuration conf) throws IOException {
    super(conf, COMPACTIONSERVER); // thread name
    this.msgInterval = conf.getInt(HConstants.COMPACTION_SERVER_MSG_INTERVAL, 3 * 1000);
    this.sleeper = new Sleeper(this.msgInterval, this);
    this.rpcServices = createRpcServices();
    setServerName(ServerName.valueOf(this.rpcServices.getSocketAddress().getHostName(),
      this.rpcServices.getSocketAddress().getPort(), this.startcode));
    if (!this.masterless) {
      masterAddressTracker = new MasterAddressTracker(getZooKeeper(), this);
      masterAddressTracker.start();
    } else {
      masterAddressTracker = null;
    }

    ZKAuthentication.loginClient(this.conf, HConstants.ZK_CLIENT_KEYTAB_FILE,
      HConstants.ZK_CLIENT_KERBEROS_PRINCIPAL, this.rpcServices.getSocketAddress().getHostName());
    // login the server principal (if using secure Hadoop)
    login(userProvider, this.rpcServices.getSocketAddress().getHostName());
    Superusers.initialize(conf);
    this.rpcServices.start();
  }

  protected void login(UserProvider user, String host) throws IOException {
    user.login(SecurityConstants.COMPACTION_SERVER_KRB_KEYTAB_FILE,
      SecurityConstants.COMPACTION_SERVER_KRB_PRINCIPAL, host);
  }

  @Override
  protected NamedQueueRecorder createNamedQueueRecord() {
    return null;
  }

  @Override
  protected void configureInfoServer(InfoServer infoServer) {

  }

  @Override
  protected Class<? extends HttpServlet> getDumpServlet() {
    return null;
  }

  @Override
  protected boolean canUpdateTableDescriptor() {
    return false;
  }

  @Override
  protected boolean cacheTableDescriptor() {
    return false;
  }

  @Override
  protected boolean clusterMode() {
    return false;
  }

  private boolean tryCompactionServerReport() throws IOException {
    CompactionServerStatusService.BlockingInterface css = cssStub;
    if (css == null) {
      return false;
    }
    try {
      CompactionServerStatusProtos.CompactionServerReportRequest.Builder request =
        CompactionServerStatusProtos.CompactionServerReportRequest.newBuilder();
      request.setServer(ProtobufUtil.toServerName(getServerName()));
      this.cssStub.compactionServerReport(null, request.build());
    } catch (ServiceException se) {
      IOException ioe = ProtobufUtil.getRemoteException(se);
      if (ioe instanceof YouAreDeadException) {
        throw ioe;
      }
      if (cssStub == css) {
        cssStub = null;
      }
      // Couldn't connect to the master, get location from zk and reconnect
      // Method blocks until new master is found or we are stopped
      createCompactionServerStatusStub();
      return false;
    }
    return true;
  }

  @Override
  public boolean isAborted() {
    return false;
  }

  @Override
  protected void stopChores() {

  }

  /**
   * The HCompactionServer sticks in this loop until closed.
   */
  @Override
  public void run() {
    if (isStopped()) {
      LOG.info("Skipping run; stopped");
      return;
    }

    try {
      preRegistrationInitialization();
    } catch (Throwable e) {
      abort("Fatal exception during initialization", e);
    }
    try {
      // We registered with the Master. Go into run mode.
      long lastMsg = System.currentTimeMillis();
      // The main run loop.
      while (!isStopped()) {
        long now = System.currentTimeMillis();
        if ((now - lastMsg) >= msgInterval) {
          tryCompactionServerReport();
          lastMsg = System.currentTimeMillis();
        }
        if (!isStopped()) {
          this.sleeper.sleep();
        }
      }
    } catch (Throwable t) {
      LOG.error("catch exception when compactionserver run ", t);
      if (!rpcServices.checkOOME(t)) {
        String prefix = t instanceof YouAreDeadException ? "" : "Unhandled: ";
        abort(prefix + t.getMessage(), t);
      }
    }
  }

  @Override
  public void abort(String reason, Throwable cause) {
    String msg = "***** ABORTING compaction server " + this + ": " + reason + " *****";
    if (cause != null) {
      LOG.error(HBaseMarkers.FATAL, msg, cause);
    } else {
      LOG.error(HBaseMarkers.FATAL, msg);
    }
    stop(msg);
  }

  @Override
  public void stop(final String msg) {
    if (!this.stopped) {
      LOG.info("***** STOPPING compaction server '" + this + "' *****");
    }
    this.stopped = true;
    LOG.info("STOPPED: " + msg);
  }

  /**
   * @see org.apache.hadoop.hbase.compactionserver.HCompactionServerCommandLine
   */
  public static void main(String[] args) {
    LOG.info("STARTING executorService " + HCompactionServer.class.getSimpleName());
    VersionInfo.logVersion();
    Configuration conf = HBaseConfiguration.create();
    @SuppressWarnings("unchecked")
    Class<? extends HCompactionServer> compactionServerClass =
      (Class<? extends HCompactionServer>) conf.getClass(HConstants.COMPACTION_SERVER_IMPL,
        HCompactionServer.class);

    new HCompactionServerCommandLine(compactionServerClass).doMain(args);
  }

  /**
   * Utility for constructing an instance of the passed HCompactionServer class.
   */
  static HCompactionServer constructCompactionServer(
    final Class<? extends HCompactionServer> compactionServerClass, final Configuration conf) {
    try {
      return (HCompactionServer) constructServer(compactionServerClass, conf);
    } catch (Exception e) {
      throw new RuntimeException(
        "Failed construction of " + "Compactionserver: " + compactionServerClass.toString(), e);
    }
  }

  @Override
  public String getClusterId() {
    return "";
  }

  @Override
  public Optional<ServerName> getActiveMaster() {
    return Optional.empty();
  }

  @Override
  public List<ServerName> getBackupMasters() {
    return List.of();
  }

  @Override
  public Iterator<ServerName> getBootstrapNodes() {
    return null;
  }

  @Override
  public List<HRegionLocation> getMetaLocations() {
    return List.of();
  }

  @Override
  public void onConfigurationChange(Configuration conf) {

  }
}
