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
package org.apache.hadoop.hbase;

import static org.apache.hadoop.hbase.ChoreService.CHORE_SERVICE_INITIAL_POOL_SIZE;
import static org.apache.hadoop.hbase.ChoreService.DEFAULT_CHORE_SERVICE_INITIAL_POOL_SIZE;
import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_SPLIT_COORDINATED_BY_ZK;
import static org.apache.hadoop.hbase.HConstants.HBASE_SPLIT_WAL_COORDINATED_BY_ZK;
import static org.apache.hadoop.hbase.compactionserver.HCompactionServer.COMPACTIONSERVER;
import static org.apache.hadoop.hbase.master.HMaster.MASTER;
import static org.apache.hadoop.hbase.regionserver.HRegionServer.MASTERLESS_CONFIG_NAME;
import static org.apache.hadoop.hbase.regionserver.HRegionServer.REGIONSERVER;

import com.google.errorprone.annotations.RestrictedApi;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import java.io.IOException;
import java.lang.management.MemoryType;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.servlet.http.HttpServlet;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.ClusterConnectionFactory;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.ConnectionRegistryEndpoint;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.coordination.ZkCoordinatedStateManager;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.http.InfoServer;
import org.apache.hadoop.hbase.io.util.MemorySizeUtil;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterRpcServicesVersionWrapper;
import org.apache.hadoop.hbase.namequeues.NamedQueueRecorder;
import org.apache.hadoop.hbase.procedure.RegionServerProcedureManagerHost;
import org.apache.hadoop.hbase.regionserver.BootstrapNodeManager;
import org.apache.hadoop.hbase.regionserver.ChunkCreator;
import org.apache.hadoop.hbase.regionserver.HeapMemoryManager;
import org.apache.hadoop.hbase.regionserver.MemStoreLAB;
import org.apache.hadoop.hbase.regionserver.RegionServerCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.ShutdownHook;
import org.apache.hadoop.hbase.regionserver.regionreplication.RegionReplicationBufferManager;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.access.AccessChecker;
import org.apache.hadoop.hbase.security.access.ZKPermissionWatcher;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.unsafe.HBasePlatformDependent;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.NettyEventLoopGroupConfig;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Sleeper;
import org.apache.hadoop.hbase.zookeeper.ClusterStatusTracker;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.ZKAuthentication;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKNodeTracker;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.BlockingRpcChannel;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos;

/**
 * Base class for hbase services, such as master or region server.
 */
@InterfaceAudience.Private
public abstract class HBaseServerBase<R extends HBaseRpcServicesBase<?>> extends Thread
  implements Server, ConfigurationObserver, ConnectionRegistryEndpoint {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseServerBase.class);

  protected final Configuration conf;

  // Go down hard. Used if file system becomes unavailable and also in
  // debugging and unit tests.
  protected final AtomicBoolean abortRequested = new AtomicBoolean(false);

  // Set when a report to the master comes back with a message asking us to
  // shutdown. Also set by call to stop when debugging or running unit tests
  // of HRegionServer in isolation.
  protected volatile boolean stopped = false;
  /**
   * Unique identifier for the cluster we are a part of.
   */
  protected String clusterId;
  protected RegionServerProcedureManagerHost rspmHost;
  protected BootstrapNodeManager bootstrapNodeManager;
  protected RegionReplicationBufferManager regionReplicationBufferManager;

  // Only for testing
  private boolean isShutdownHookInstalled = false;

  /**
   * This servers startcode.
   */
  protected final long startcode;

  protected final UserProvider userProvider;

  // zookeeper connection and watcher
  protected final ZKWatcher zooKeeper;

  /**
   * The server name the Master sees us as. Its made from the hostname the master passes us, port,
   * and server startcode. Gets set after registration against Master.
   */
  protected ServerName serverName;

  protected final R rpcServices;

  /**
   * hostname specified by hostname config
   */
  protected final String useThisHostnameInstead;

  /**
   * Provide online slow log responses from ringbuffer
   */
  protected final NamedQueueRecorder namedQueueRecorder;

  /**
   * Configuration manager is used to register/deregister and notify the configuration observers
   * when the regionserver is notified that there was a change in the on disk configs.
   */
  protected final ConfigurationManager configurationManager;

  /**
   * ChoreService used to schedule tasks that we want to run periodically
   */
  protected final ChoreService choreService;

  // Instance of the hbase executor executorService.
  protected final ExecutorService executorService;

  // Cluster Status Tracker
  protected final ClusterStatusTracker clusterStatusTracker;

  protected final CoordinatedStateManager csm;

  // Info server. Default access so can be used by unit tests. REGIONSERVER
  // is name of the webapp and the attribute name used stuffing this instance
  // into web context.
  protected InfoServer infoServer;

  protected HFileSystem dataFs;

  protected HFileSystem walFs;

  protected Path dataRootDir;

  protected Path walRootDir;

  protected int msgInterval;

  // A sleeper that sleeps for msgInterval.
  protected Sleeper sleeper;

  /**
   * Go here to get table descriptors.
   */
  protected TableDescriptors tableDescriptors;

  /**
   * The asynchronous cluster connection to be shared by services.
   */
  protected AsyncClusterConnection asyncClusterConnection;

  /**
   * Cache for the meta region replica's locations. Also tracks their changes to avoid stale cache
   * entries. Used for serving ClientMetaService.
   */
  protected final MetaRegionLocationCache metaRegionLocationCache;

  protected final NettyEventLoopGroupConfig eventLoopGroupConfig;

  // RPC client. Used to make the stub above that does region server status checking.
  protected RpcClient rpcClient;

  // master address tracker
  protected MasterAddressTracker masterAddressTracker;
  protected int shortOperationTimeout;
  /**
   * True if this RegionServer is coming up in a cluster where there is no Master; means it needs to
   * just come up and make do without a Master to talk to: e.g. in test or HRegionServer is doing
   * other than its usual duties: e.g. as an hollowed-out host whose only purpose is as a
   * Replication-stream sink; see HBASE-18846 for more. TODO: can this replace
   * {@link org.apache.hadoop.hbase.regionserver.HRegionServer#TEST_SKIP_REPORTING_TRANSITION} ?
   */
  protected final boolean masterless;

  // flag set after we're done setting up server threads
  protected final AtomicBoolean online = new AtomicBoolean(false);

  protected volatile boolean dataFsOk;

  public AtomicBoolean getOnline() {
    return online;
  }

  /**
   * Report the status of the server. A server is online once all the startup is completed (setting
   * up filesystem, starting executorService threads, etc.). This method is designed mostly to be
   * useful in tests.
   * @return true if online, false if not.
   */
  public boolean isOnline() {
    return online.get();
  }

  public void waitForServerOnline() {
    while (!isStopped() && !isOnline()) {
      synchronized (online) {
        try {
          online.wait(msgInterval);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
  }

  private void setupSignalHandlers() {
    if (!SystemUtils.IS_OS_WINDOWS) {
      HBasePlatformDependent.handle("HUP", (number, name) -> {
        try {
          updateConfiguration();
        } catch (IOException e) {
          LOG.error("Problem while reloading configuration", e);
        }
      });
    }
  }

  /**
   * Setup our cluster connection if not already initialized.
   */
  protected final synchronized void setupClusterConnection() throws IOException {
    if (asyncClusterConnection == null) {
      InetSocketAddress localAddress =
        new InetSocketAddress(rpcServices.getSocketAddress().getAddress(), 0);
      User user = userProvider.getCurrent();
      asyncClusterConnection =
        ClusterConnectionFactory.createAsyncClusterConnection(this, conf, localAddress, user);
    }
  }

  protected final void initializeFileSystem() throws IOException {
    // Get fs instance used by this RS. Do we use checksum verification in the hbase? If hbase
    // checksum verification enabled, then automatically switch off hdfs checksum verification.
    boolean useHBaseChecksum = conf.getBoolean(HConstants.HBASE_CHECKSUM_VERIFICATION, true);
    String walDirUri = CommonFSUtils.getDirUri(this.conf,
      new Path(conf.get(CommonFSUtils.HBASE_WAL_DIR, conf.get(HConstants.HBASE_DIR))));
    // set WAL's uri
    if (walDirUri != null) {
      CommonFSUtils.setFsDefault(this.conf, walDirUri);
    }
    // init the WALFs
    this.walFs = new HFileSystem(this.conf, useHBaseChecksum);
    this.walRootDir = CommonFSUtils.getWALRootDir(this.conf);
    // Set 'fs.defaultFS' to match the filesystem on hbase.rootdir else
    // underlying hadoop hdfs accessors will be going against wrong filesystem
    // (unless all is set to defaults).
    String rootDirUri =
      CommonFSUtils.getDirUri(this.conf, new Path(conf.get(HConstants.HBASE_DIR)));
    if (rootDirUri != null) {
      CommonFSUtils.setFsDefault(this.conf, rootDirUri);
    }
    // init the filesystem
    this.dataFs = new HFileSystem(this.conf, useHBaseChecksum);
    this.dataRootDir = CommonFSUtils.getRootDir(this.conf);
    int tableDescriptorParallelLoadThreads =
      conf.getInt("hbase.tabledescriptor.parallel.load.threads", 0);
    this.tableDescriptors = new FSTableDescriptors(this.dataFs, this.dataRootDir,
      !canUpdateTableDescriptor(), cacheTableDescriptor(), tableDescriptorParallelLoadThreads);
  }

  public HBaseServerBase(Configuration conf, String name) throws IOException {
    super(name); // thread name
    final Span span = TraceUtil.createSpan("HBaseServerBase.cxtor");
    try (Scope ignored = span.makeCurrent()) {
      this.conf = conf;
      this.eventLoopGroupConfig =
        NettyEventLoopGroupConfig.setup(conf, getClass().getSimpleName() + "-EventLoopGroup");
      this.startcode = EnvironmentEdgeManager.currentTime();
      this.userProvider = UserProvider.instantiate(conf);
      this.msgInterval = conf.getInt("hbase.regionserver.msginterval", 3 * 1000);
      this.sleeper = new Sleeper(this.msgInterval, this);
      this.namedQueueRecorder = createNamedQueueRecord();
      this.rpcServices = createRpcServices();
      useThisHostnameInstead = getUseThisHostnameInstead(conf);
      InetSocketAddress addr = rpcServices.getSocketAddress();

      // if use-ip is enabled, we will use ip to expose Master/RS service for client,
      // see HBASE-27304 for details.
      boolean useIp = conf.getBoolean(HConstants.HBASE_SERVER_USEIP_ENABLED_KEY,
        HConstants.HBASE_SERVER_USEIP_ENABLED_DEFAULT);
      String isaHostName =
        useIp ? addr.getAddress().getHostAddress() : addr.getAddress().getHostName();
      String hostName =
        StringUtils.isBlank(useThisHostnameInstead) ? isaHostName : useThisHostnameInstead;
      serverName = ServerName.valueOf(hostName, addr.getPort(), this.startcode);
      // login the zookeeper client principal (if using security)
      ZKAuthentication.loginClient(this.conf, HConstants.ZK_CLIENT_KEYTAB_FILE,
        HConstants.ZK_CLIENT_KERBEROS_PRINCIPAL, hostName);
      // login the server principal (if using secure Hadoop)
      login(userProvider, hostName);
      // init superusers and add the server principal (if using security)
      // or process owner as default super user.
      Superusers.initialize(conf);
      zooKeeper =
        new ZKWatcher(conf, getProcessName() + ":" + addr.getPort(), this, canCreateBaseZNode());

      this.configurationManager = new ConfigurationManager();
      setupSignalHandlers();

      initializeFileSystem();

      int choreServiceInitialSize =
        conf.getInt(CHORE_SERVICE_INITIAL_POOL_SIZE, DEFAULT_CHORE_SERVICE_INITIAL_POOL_SIZE);
      this.choreService = new ChoreService(getName(), choreServiceInitialSize, true);
      this.executorService = new ExecutorService(getName());

      this.metaRegionLocationCache = new MetaRegionLocationCache(zooKeeper);
      if (clusterMode()) {
        if (
          conf.getBoolean(HBASE_SPLIT_WAL_COORDINATED_BY_ZK, DEFAULT_HBASE_SPLIT_COORDINATED_BY_ZK)
        ) {
          csm = new ZkCoordinatedStateManager(this);
        } else {
          csm = null;
        }
        clusterStatusTracker = new ClusterStatusTracker(zooKeeper, this);
        clusterStatusTracker.start();
      } else {
        csm = null;
        clusterStatusTracker = null;
      }
      putUpWebUI();
      this.shortOperationTimeout = conf.getInt(HConstants.HBASE_RPC_SHORTOPERATION_TIMEOUT_KEY,
        HConstants.DEFAULT_HBASE_RPC_SHORTOPERATION_TIMEOUT);
      this.masterless = conf.getBoolean(MASTERLESS_CONFIG_NAME, false);
      if (!this.masterless) {
        masterAddressTracker = new MasterAddressTracker(getZooKeeper(), this);
        masterAddressTracker.start();
      } else {
        masterAddressTracker = null;
      }
      this.dataFsOk = true;
      span.setStatus(StatusCode.OK);
    } catch (Throwable t) {
      TraceUtil.setError(span, t);
      throw t;
    } finally {
      span.end();
    }
  }

  /**
   * Puts up the webui.
   */
  protected void putUpWebUI() throws IOException {
    int port =
      this.conf.getInt(HConstants.REGIONSERVER_INFO_PORT, HConstants.DEFAULT_REGIONSERVER_INFOPORT);
    String addr = this.conf.get("hbase.regionserver.info.bindAddress", "0.0.0.0");

    if (this instanceof HMaster) {
      port = conf.getInt(HConstants.MASTER_INFO_PORT, HConstants.DEFAULT_MASTER_INFOPORT);
      addr = this.conf.get("hbase.master.info.bindAddress", "0.0.0.0");
    }
    // -1 is for disabling info server
    if (port < 0) {
      return;
    }

    if (!Addressing.isLocalAddress(InetAddress.getByName(addr))) {
      String msg = "Failed to start http info server. Address " + addr
        + " does not belong to this host. Correct configuration parameter: "
        + "hbase.regionserver.info.bindAddress";
      LOG.error(msg);
      throw new IOException(msg);
    }
    // check if auto port bind enabled
    boolean auto = this.conf.getBoolean(HConstants.REGIONSERVER_INFO_PORT_AUTO, false);
    tryCreateInfoServer(addr, port, auto);
    port = this.infoServer.getPort();
    conf.setInt(HConstants.REGIONSERVER_INFO_PORT, port);
    int masterInfoPort =
      conf.getInt(HConstants.MASTER_INFO_PORT, HConstants.DEFAULT_MASTER_INFOPORT);
    conf.setInt("hbase.master.info.port.orig", masterInfoPort);
    conf.setInt(HConstants.MASTER_INFO_PORT, port);
  }

  protected void tryCreateInfoServer(String addr, int port, boolean auto) throws IOException {
    while (true) {
      try {
        this.infoServer = new InfoServer(getProcessName(), addr, port, false, this.conf);
        infoServer.addPrivilegedServlet("dump", "/dump", getDumpServlet());
        configureInfoServer(infoServer);
        this.infoServer.start();
        break;
      } catch (BindException e) {
        if (!auto) {
          // auto bind disabled throw BindException
          LOG.error("Failed binding http info server to port: " + port);
          throw e;
        }
        // auto bind enabled, try to use another port
        LOG.info("Failed binding http info server to port: " + port);
        port++;
        LOG.info("Retry starting http info server with port: " + port);
      }
    }
  }

  /**
   * Sets the abort state if not already set.
   * @return True if abortRequested set to True successfully, false if an abort is already in
   *         progress.
   */
  protected final boolean setAbortRequested() {
    return abortRequested.compareAndSet(false, true);
  }

  @Override
  public boolean isStopped() {
    return stopped;
  }

  @Override
  public boolean isAborted() {
    return abortRequested.get();
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public AsyncClusterConnection getAsyncClusterConnection() {
    return asyncClusterConnection;
  }

  @Override
  public ZKWatcher getZooKeeper() {
    return zooKeeper;
  }

  protected final void shutdownChore(ScheduledChore chore) {
    if (chore != null) {
      chore.shutdown();
    }
  }

  protected final void initializeMemStoreChunkCreator(HeapMemoryManager hMemManager) {
    if (MemStoreLAB.isEnabled(conf)) {
      // MSLAB is enabled. So initialize MemStoreChunkPool
      // By this time, the MemstoreFlusher is already initialized. We can get the global limits from
      // it.
      Pair<Long, MemoryType> pair = MemorySizeUtil.getGlobalMemStoreSize(conf);
      long globalMemStoreSize = pair.getFirst();
      boolean offheap = pair.getSecond() == MemoryType.NON_HEAP;
      // When off heap memstore in use, take full area for chunk pool.
      float poolSizePercentage = offheap
        ? 1.0F
        : conf.getFloat(MemStoreLAB.CHUNK_POOL_MAXSIZE_KEY, MemStoreLAB.POOL_MAX_SIZE_DEFAULT);
      float initialCountPercentage = conf.getFloat(MemStoreLAB.CHUNK_POOL_INITIALSIZE_KEY,
        MemStoreLAB.POOL_INITIAL_SIZE_DEFAULT);
      int chunkSize = conf.getInt(MemStoreLAB.CHUNK_SIZE_KEY, MemStoreLAB.CHUNK_SIZE_DEFAULT);
      float indexChunkSizePercent = conf.getFloat(MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_KEY,
        MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_DEFAULT);
      // init the chunkCreator
      ChunkCreator.initialize(chunkSize, offheap, globalMemStoreSize, poolSizePercentage,
        initialCountPercentage, hMemManager, indexChunkSizePercent);
    }
  }

  protected abstract void stopChores();

  protected final void stopChoreService() {
    // clean up the scheduled chores
    if (choreService != null) {
      LOG.info("Shutdown chores and chore service");
      stopChores();
      // cancel the remaining scheduled chores (in case we missed out any)
      // TODO: cancel will not cleanup the chores, so we need make sure we do not miss any
      choreService.shutdown();
    }
  }

  protected final void stopExecutorService() {
    if (executorService != null) {
      LOG.info("Shutdown executor service");
      executorService.shutdown();
    }
  }

  protected final void closeClusterConnection() {
    if (asyncClusterConnection != null) {
      LOG.info("Close async cluster connection");
      try {
        this.asyncClusterConnection.close();
      } catch (IOException e) {
        // Although the {@link Closeable} interface throws an {@link
        // IOException}, in reality, the implementation would never do that.
        LOG.warn("Attempt to close server's AsyncClusterConnection failed.", e);
      }
    }
  }

  protected final void stopInfoServer() {
    if (this.infoServer != null) {
      LOG.info("Stop info server");
      try {
        this.infoServer.stop();
      } catch (Exception e) {
        LOG.error("Failed to stop infoServer", e);
      }
    }
  }

  protected final void closeZooKeeper() {
    if (this.zooKeeper != null) {
      LOG.info("Close zookeeper");
      this.zooKeeper.close();
    }
  }

  protected final void closeTableDescriptors() {
    if (this.tableDescriptors != null) {
      LOG.info("Close table descriptors");
      try {
        this.tableDescriptors.close();
      } catch (IOException e) {
        LOG.debug("Failed to close table descriptors gracefully", e);
      }
    }
  }

  /**
   * In order to register ShutdownHook, this method is called when HMaster and HRegionServer are
   * started. For details, please refer to HBASE-26951
   */
  protected final void installShutdownHook() {
    ShutdownHook.install(conf, dataFs, this, Thread.currentThread());
    isShutdownHookInstalled = true;
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  public boolean isShutdownHookInstalled() {
    return isShutdownHookInstalled;
  }

  @Override
  public ServerName getServerName() {
    return serverName;
  }

  @Override
  public ChoreService getChoreService() {
    return choreService;
  }

  /** Returns Return table descriptors implementation. */
  public TableDescriptors getTableDescriptors() {
    return this.tableDescriptors;
  }

  public ExecutorService getExecutorService() {
    return executorService;
  }

  public AccessChecker getAccessChecker() {
    return rpcServices.getAccessChecker();
  }

  public ZKPermissionWatcher getZKPermissionWatcher() {
    return rpcServices.getZkPermissionWatcher();
  }

  @Override
  public CoordinatedStateManager getCoordinatedStateManager() {
    return csm;
  }

  @Override
  public Connection createConnection(Configuration conf) throws IOException {
    User user = UserProvider.instantiate(conf).getCurrent();
    return ConnectionFactory.createConnection(conf, null, user);
  }

  /** Returns Return the rootDir. */
  public Path getDataRootDir() {
    return dataRootDir;
  }

  @Override
  public FileSystem getFileSystem() {
    return dataFs;
  }

  /** Returns Return the walRootDir. */
  public Path getWALRootDir() {
    return walRootDir;
  }

  /** Returns Return the walFs. */
  public FileSystem getWALFileSystem() {
    return walFs;
  }

  /**
   * Bring up connection to zk ensemble and then wait until a master for this cluster and then after
   * that, wait until cluster 'up' flag has been set. This is the order in which master does things.
   * <p>
   * Finally open long-living server short-circuit connection.
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE",
      justification = "cluster Id znode read would give us correct response")
  protected void initializeZooKeeper() throws IOException, InterruptedException {
    // Nothing to do in here if no Master in the mix.
    if (this.masterless) {
      return;
    }

    // Create the master address tracker, register with zk, and start it. Then
    // block until a master is available. No point in starting up if no master
    // running.
    blockAndCheckIfStopped(this.masterAddressTracker);

    // Wait on cluster being up. Master will set this flag up in zookeeper
    // when ready.
    blockAndCheckIfStopped(this.clusterStatusTracker);

    // If we are HMaster then the cluster id should have already been set.
    if (clusterId == null) {
      // Retrieve clusterId
      // Since cluster status is now up
      // ID should have already been set by HMaster
      try {
        clusterId = ZKClusterId.readClusterIdZNode(this.zooKeeper);
        if (clusterId == null) {
          this.abort("Cluster ID has not been set");
        }
        LOG.info("ClusterId : " + clusterId);
      } catch (KeeperException e) {
        this.abort("Failed to retrieve Cluster ID", e);
      }
    }

    if (isStopped() || isAborted()) {
      return; // No need for further initialization
    }

    // watch for snapshots and other procedures
    try {
      rspmHost = new RegionServerProcedureManagerHost();
      rspmHost.loadProcedures(conf);
      RegionServerServices regionServerServices = getRegionServerServices();
      if (null != regionServerServices) {
        rspmHost.initialize(regionServerServices);
      }
    } catch (KeeperException e) {
      this.abort("Failed to reach coordination cluster when creating procedure handler.", e);
    }
  }

  protected RegionServerServices getRegionServerServices() {
    return null;
  }

  /**
   * Utilty method to wait indefinitely on a znode availability while checking if the region server
   * is shut down
   * @param tracker znode tracker to use
   * @throws IOException          any IO exception, plus if the RS is stopped
   * @throws InterruptedException if the waiting thread is interrupted
   */
  protected void blockAndCheckIfStopped(ZKNodeTracker tracker)
    throws IOException, InterruptedException {
    //TODO(chan) check why null check not added
    if(null == tracker)
    {
      return;
    }
    while (tracker.blockUntilAvailable(this.msgInterval, false) == null) {
      if (this.stopped) {
        throw new IOException("Received the shutdown message while waiting.");
      }
    }
  }

  /**
   * All initialization needed before we go register with Master.<br>
   * Do bare minimum. Do bulk of initializations AFTER we've connected to the Master.<br>
   * In here we just put up the RpcServer, setup Connection, and ZooKeeper.
   */
  protected void preRegistrationInitialization() {
    final Span span = TraceUtil.createSpan("HRegionServer.preRegistrationInitialization");
    try (Scope ignored = span.makeCurrent()) {
      initializeZooKeeper();
      setupClusterConnection();
      bootstrapNodeManager = new BootstrapNodeManager(asyncClusterConnection, masterAddressTracker);
      RegionServerServices regionServerServices = getRegionServerServices();
      if (null != regionServerServices) {
        regionReplicationBufferManager = new RegionReplicationBufferManager(regionServerServices);
      }
      // Setup RPC client for master communication
      this.rpcClient = asyncClusterConnection.getRpcClient();
      span.setStatus(StatusCode.OK);
    } catch (Throwable t) {
      // Call stop if error or process will stick around for ever since server
      // puts up non-daemon threads.
      TraceUtil.setError(span, t);
      getRpcServices().stop();
      abort("Initialization of RS failed.  Hence aborting RS.", t);
    } finally {
      span.end();
    }
  }

  /** Returns True if the cluster is up. */
  public boolean isClusterUp() {
    return !clusterMode() || this.clusterStatusTracker.isClusterUp();
  }

  /** Returns time stamp in millis of when this server was started */
  public long getStartcode() {
    return this.startcode;
  }

  public InfoServer getInfoServer() {
    return infoServer;
  }

  public int getMsgInterval() {
    return msgInterval;
  }

  /**
   * get NamedQueue Provider to add different logs to ringbuffer
   */
  public NamedQueueRecorder getNamedQueueRecorder() {
    return this.namedQueueRecorder;
  }

  public RpcServerInterface getRpcServer() {
    return rpcServices.getRpcServer();
  }

  public NettyEventLoopGroupConfig getEventLoopGroupConfig() {
    return eventLoopGroupConfig;
  }

  public R getRpcServices() {
    return rpcServices;
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  public MetaRegionLocationCache getMetaRegionLocationCache() {
    return this.metaRegionLocationCache;
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  public ConfigurationManager getConfigurationManager() {
    return configurationManager;
  }

  /**
   * Reload the configuration from disk.
   */
  public void updateConfiguration() throws IOException {
    LOG.info("Reloading the configuration from disk.");
    // Reload the configuration from disk.
    preUpdateConfiguration();
    conf.reloadConfiguration();
    configurationManager.notifyAllObservers(conf);
    postUpdateConfiguration();
  }

  private void preUpdateConfiguration() throws IOException {
    CoprocessorHost<?, ?> coprocessorHost = getCoprocessorHost();
    if (coprocessorHost instanceof RegionServerCoprocessorHost) {
      ((RegionServerCoprocessorHost) coprocessorHost).preUpdateConfiguration(conf);
    } else if (coprocessorHost instanceof MasterCoprocessorHost) {
      ((MasterCoprocessorHost) coprocessorHost).preUpdateConfiguration(conf);
    }
  }

  private void postUpdateConfiguration() throws IOException {
    CoprocessorHost<?, ?> coprocessorHost = getCoprocessorHost();
    if (coprocessorHost instanceof RegionServerCoprocessorHost) {
      ((RegionServerCoprocessorHost) coprocessorHost).postUpdateConfiguration(conf);
    } else if (coprocessorHost instanceof MasterCoprocessorHost) {
      ((MasterCoprocessorHost) coprocessorHost).postUpdateConfiguration(conf);
    }
  }

  @Override
  public String toString() {
    return getServerName().toString();
  }

  protected abstract CoprocessorHost<?, ?> getCoprocessorHost();

  protected abstract boolean canCreateBaseZNode();

  protected abstract String getProcessName();

  protected abstract R createRpcServices() throws IOException;

  protected abstract String getUseThisHostnameInstead(Configuration conf) throws IOException;

  protected abstract void login(UserProvider user, String host) throws IOException;

  protected abstract NamedQueueRecorder createNamedQueueRecord();

  protected abstract void configureInfoServer(InfoServer infoServer);

  protected abstract Class<? extends HttpServlet> getDumpServlet();

  protected abstract boolean canUpdateTableDescriptor();

  protected abstract boolean cacheTableDescriptor();

  protected abstract boolean clusterMode();

  public void setServerName(ServerName serverName) {
    this.serverName = serverName;
  }

  protected static boolean sleepInterrupted(long millis) {
    boolean interrupted = false;
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while sleeping");
      interrupted = true;
    }
    return interrupted;
  }

  /**
   * Get the current master from ZooKeeper and open the RPC connection to it. To get a fresh
   * connection, the current rssStub must be null. Method will block until a master is available.
   * You can break from this block by requesting the server stop.
   * @param tClass  the protobuf generated service class
   * @param refresh If true then master address will be read from ZK, otherwise use cached data
   * @return the BlockingInterface of protobuf generated service class
   */
  @InterfaceAudience.Private
  protected synchronized <T extends org.apache.hbase.thirdparty.com.google.protobuf.Service> Object
    createMasterStub(Class<T> tClass, boolean refresh) {
    ServerName sn;
    long previousLogTime = 0;
    boolean interrupted = false;
    try {
      while (keepLooping()) {
        sn = this.masterAddressTracker.getMasterAddress(refresh);
        if (sn == null) {
          if (!keepLooping()) {
            // give up with no connection.
            LOG.debug("No master found and cluster is stopped; bailing out");
            return null;
          }
          if (System.currentTimeMillis() > (previousLogTime + 1000)) {
            LOG.debug("No master found; retry");
            previousLogTime = System.currentTimeMillis();
          }
          refresh = true; // let's try pull it from ZK directly
          if (sleepInterrupted(200)) {
            interrupted = true;
          }
          continue;
        }

        // If we are on the active master, use the shortcut
        if (this instanceof HMaster && sn.equals(getServerName())) {
          // Wrap the shortcut in a class providing our version to the calls where it's relevant.
          // Normally, RpcServer-based threadlocals do that.
          if (
            tClass.getName()
              .equals(RegionServerStatusProtos.RegionServerStatusService.class.getName())
          ) {
            return new MasterRpcServicesVersionWrapper(((HMaster) this).getMasterRpcServices());
          }
          return ((HMaster) this).getMasterRpcServices();
        }
        try {
          BlockingRpcChannel channel = this.rpcClient.createBlockingRpcChannel(sn,
            userProvider.getCurrent(), shortOperationTimeout);
          try {
            Method newBlockingStubMethod =
              tClass.getMethod("newBlockingStub", BlockingRpcChannel.class);
            return newBlockingStubMethod.invoke(null, channel);
          } catch (IllegalArgumentException | IllegalAccessException | InvocationTargetException
            | NoSuchMethodException ite) {
            LOG.error("Unable to create class {}, master is {}", tClass.getName(), sn, ite);
            return null;
          }
        } catch (IOException e) {
          if (System.currentTimeMillis() > (previousLogTime + 1000)) {
            e = e instanceof RemoteException ? ((RemoteException) e).unwrapRemoteException() : e;
            if (e instanceof ServerNotRunningYetException) {
              LOG.info("Master {} isn't available yet, retrying", sn);
            } else {
              LOG.warn("Unable to connect to master {} . Retrying. Error was:", sn, e);
            }
            previousLogTime = System.currentTimeMillis();
          }
          if (sleepInterrupted(200)) {
            interrupted = true;
          }
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
    return null;
  }

  /**
   * @return True if we should break loop because cluster is going down or this server has been
   *         stopped or hdfs has gone bad.
   */
  protected boolean keepLooping() {
    return !this.stopped && isClusterUp();
  }

  /**
   * Utility for constructing an instance of the passed HBaseServerBase subclass.
   */
  protected static HBaseServerBase constructServer(
    final Class<? extends HBaseServerBase> serverClass, final Configuration conf) throws Exception {
    Constructor<? extends HBaseServerBase> c = serverClass.getConstructor(Configuration.class);
    return c.newInstance(conf);
  }

  /**
   * Checks to see if the file system is still accessible. If not, sets abortRequested and
   * stopRequested
   * @return false if file system is not available
   */
  public boolean checkFileSystem() {
    if (this.dataFsOk && this.dataFs != null) {
      try {
        FSUtils.checkFileSystemAvailable(this.dataFs);
      } catch (IOException e) {
        abort("File System not available", e);
        this.dataFsOk = false;
      }
    }
    return this.dataFsOk;
  }

  public ServerType getServerType() {
    String processName = getProcessName();
    if (processName.equals(MASTER)) {
      return ServerType.Master;
    }
    if (processName.equals(REGIONSERVER)) {
      return ServerType.RegionServer;
    }
    if (processName.equals(COMPACTIONSERVER)) {
      return ServerType.CompactionServer;
    }
    return ServerType.ReplicationServer;
  }

}
