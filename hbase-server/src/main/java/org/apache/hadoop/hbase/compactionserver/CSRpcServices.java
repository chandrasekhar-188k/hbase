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

import static org.apache.hadoop.hbase.HConstants.COMPACTION_SERVER_PORT;
import static org.apache.hadoop.hbase.HConstants.DEFAULT_COMPACTION_SERVER_PORT;
import static org.apache.hadoop.hbase.compactionserver.HCompactionServer.COMPACTIONSERVER;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseRpcServicesBase;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.ipc.PriorityFunction;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.regionserver.SimpleRpcSchedulerFactory;
import org.apache.hadoop.hbase.util.DNS;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.CompactionProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.CompactionProtos.CompactResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.CompactionProtos.CompactionService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos;

@InterfaceAudience.Private
public class CSRpcServices extends HBaseRpcServicesBase<HCompactionServer>
  implements CompactionService.BlockingInterface {
  protected static final Logger LOG = LoggerFactory.getLogger(CSRpcServices.class);

  private final HCompactionServer compactionServer;

  /** RPC scheduler to use for the compaction server. */
  public static final String COMPACTION_SERVER_RPC_SCHEDULER_FACTORY_CLASS =
    "hbase.compaction.server.rpc.scheduler.factory.class";

  void start() {
    rpcServer.start();
  }

  @Override
  protected boolean defaultReservoirEnabled() {
    return false;
  }

  @Override
  protected DNS.ServerType getDNSServerType() {
    return DNS.ServerType.COMPACTIONSERVER;
  }

  @Override
  protected String getHostname(Configuration conf, String defaultHostname) {
    return conf.get("hbase.compactionserver.ipc.address", defaultHostname);
  }

  @Override
  protected String getPortConfigName() {
    return COMPACTION_SERVER_PORT;
  }

  @Override
  protected int getDefaultPort() {
    return DEFAULT_COMPACTION_SERVER_PORT;
  }

  @Override
  protected PriorityFunction createPriority() {
    return new CSAnnotationReadingPriorityFunction(this);
  }

  protected Class<?> getRpcSchedulerFactoryClass(Configuration conf) {
    return conf.getClass(COMPACTION_SERVER_RPC_SCHEDULER_FACTORY_CLASS,
      SimpleRpcSchedulerFactory.class);
  }

  @Override
  protected List<RpcServer.BlockingServiceAndInterface> getServices() {
    List<RpcServer.BlockingServiceAndInterface> bssi = new ArrayList<>();
    bssi.add(new RpcServer.BlockingServiceAndInterface(
      CompactionService.newReflectiveBlockingService(this),
      CompactionService.BlockingInterface.class));
    return new ImmutableList.Builder<RpcServer.BlockingServiceAndInterface>().addAll(bssi).build();
  }

  CSRpcServices(final HCompactionServer cs) throws IOException {
    super(cs, COMPACTIONSERVER);
    compactionServer = cs;
  }

  @Override
  public AdminProtos.GetRegionInfoResponse getRegionInfo(RpcController rpcController,
    AdminProtos.GetRegionInfoRequest getRegionInfoRequest) throws ServiceException {
    throw new ServiceException(
      new DoNotRetryIOException("Unsupported method on compaction server"));
  }

  @Override
  public AdminProtos.GetStoreFileResponse getStoreFile(RpcController rpcController,
    AdminProtos.GetStoreFileRequest getStoreFileRequest) throws ServiceException {
    throw new ServiceException(
      new DoNotRetryIOException("Unsupported method on compaction server"));
  }

  @Override
  public AdminProtos.GetOnlineRegionResponse getOnlineRegion(RpcController rpcController,
    AdminProtos.GetOnlineRegionRequest getOnlineRegionRequest) throws ServiceException {
    throw new ServiceException(
      new DoNotRetryIOException("Unsupported method on compaction server"));
  }

  @Override
  public AdminProtos.OpenRegionResponse openRegion(RpcController rpcController,
    AdminProtos.OpenRegionRequest openRegionRequest) throws ServiceException {
    throw new ServiceException(
      new DoNotRetryIOException("Unsupported method on compaction server"));
  }

  @Override
  public AdminProtos.WarmupRegionResponse warmupRegion(RpcController rpcController,
    AdminProtos.WarmupRegionRequest warmupRegionRequest) throws ServiceException {
    throw new ServiceException(
      new DoNotRetryIOException("Unsupported method on compaction server"));
  }

  @Override
  public AdminProtos.CloseRegionResponse closeRegion(RpcController rpcController,
    AdminProtos.CloseRegionRequest closeRegionRequest) throws ServiceException {
    throw new ServiceException(
      new DoNotRetryIOException("Unsupported method on compaction server"));
  }

  @Override
  public AdminProtos.FlushRegionResponse flushRegion(RpcController rpcController,
    AdminProtos.FlushRegionRequest flushRegionRequest) throws ServiceException {
    throw new ServiceException(
      new DoNotRetryIOException("Unsupported method on compaction server"));
  }

  @Override
  public AdminProtos.CompactionSwitchResponse compactionSwitch(RpcController rpcController,
    AdminProtos.CompactionSwitchRequest compactionSwitchRequest) throws ServiceException {
    throw new ServiceException(
      new DoNotRetryIOException("Unsupported method on compaction server"));
  }

  @Override
  public AdminProtos.CompactRegionResponse compactRegion(RpcController rpcController,
    AdminProtos.CompactRegionRequest compactRegionRequest) throws ServiceException {
    throw new ServiceException(
      new DoNotRetryIOException("Unsupported method on compaction server"));
  }

  @Override
  public AdminProtos.ReplicateWALEntryResponse replicateWALEntry(RpcController rpcController,
    AdminProtos.ReplicateWALEntryRequest replicateWALEntryRequest) throws ServiceException {
    throw new ServiceException(
      new DoNotRetryIOException("Unsupported method on compaction server"));
  }

  @Override
  public AdminProtos.ReplicateWALEntryResponse replay(RpcController rpcController,
    AdminProtos.ReplicateWALEntryRequest replicateWALEntryRequest) throws ServiceException {
    throw new ServiceException(
      new DoNotRetryIOException("Unsupported method on compaction server"));
  }

  @Override
  public AdminProtos.ReplicateWALEntryResponse replicateToReplica(RpcController rpcController,
    AdminProtos.ReplicateWALEntryRequest replicateWALEntryRequest) throws ServiceException {
    throw new ServiceException(
      new DoNotRetryIOException("Unsupported method on compaction server"));
  }

  @Override
  public AdminProtos.RollWALWriterResponse rollWALWriter(RpcController rpcController,
    AdminProtos.RollWALWriterRequest rollWALWriterRequest) throws ServiceException {
    throw new ServiceException(
      new DoNotRetryIOException("Unsupported method on compaction server"));
  }

  @Override
  public AdminProtos.GetServerInfoResponse getServerInfo(RpcController rpcController,
    AdminProtos.GetServerInfoRequest getServerInfoRequest) throws ServiceException {
    throw new ServiceException(
      new DoNotRetryIOException("Unsupported method on compaction server"));
  }

  @Override
  public AdminProtos.StopServerResponse stopServer(RpcController rpcController,
    AdminProtos.StopServerRequest stopServerRequest) throws ServiceException {
    throw new ServiceException(
      new DoNotRetryIOException("Unsupported method on compaction server"));
  }

  @Override
  public AdminProtos.UpdateFavoredNodesResponse updateFavoredNodes(RpcController rpcController,
    AdminProtos.UpdateFavoredNodesRequest updateFavoredNodesRequest) throws ServiceException {
    throw new ServiceException(
      new DoNotRetryIOException("Unsupported method on compaction server"));
  }

  @Override
  public AdminProtos.GetRegionLoadResponse getRegionLoad(RpcController rpcController,
    AdminProtos.GetRegionLoadRequest getRegionLoadRequest) throws ServiceException {
    throw new ServiceException(
      new DoNotRetryIOException("Unsupported method on compaction server"));
  }

  @Override
  public AdminProtos.ClearCompactionQueuesResponse clearCompactionQueues(
    RpcController rpcController,
    AdminProtos.ClearCompactionQueuesRequest clearCompactionQueuesRequest) throws ServiceException {
    throw new ServiceException(
      new DoNotRetryIOException("Unsupported method on compaction server"));
  }

  @Override
  public AdminProtos.ClearRegionBlockCacheResponse clearRegionBlockCache(
    RpcController rpcController,
    AdminProtos.ClearRegionBlockCacheRequest clearRegionBlockCacheRequest) throws ServiceException {
    throw new ServiceException(
      new DoNotRetryIOException("Unsupported method on compaction server"));
  }

  @Override
  public QuotaProtos.GetSpaceQuotaSnapshotsResponse getSpaceQuotaSnapshots(
    RpcController rpcController,
    QuotaProtos.GetSpaceQuotaSnapshotsRequest getSpaceQuotaSnapshotsRequest)
    throws ServiceException {
    throw new ServiceException(
      new DoNotRetryIOException("Unsupported method on compaction server"));
  }

  @Override
  public AdminProtos.ExecuteProceduresResponse executeProcedures(RpcController rpcController,
    AdminProtos.ExecuteProceduresRequest executeProceduresRequest) throws ServiceException {
    throw new ServiceException(
      new DoNotRetryIOException("Unsupported method on compaction server"));
  }

  @Override
  public CompactionProtos.CompleteCompactionResponse completeCompaction(RpcController controller,
    CompactionProtos.CompleteCompactionRequest request) throws ServiceException {
    throw new ServiceException(
      new DoNotRetryIOException("Unsupported method on compaction server"));
  }

  @Override
  public AdminProtos.GetCachedFilesListResponse getCachedFilesList(RpcController rpcController,
    AdminProtos.GetCachedFilesListRequest getCachedFilesListRequest) throws ServiceException {
    throw new ServiceException(
      new DoNotRetryIOException("Unsupported method on compaction server"));
  }

  /**
   * Request compaction on the compaction server.
   * @param controller the RPC controller
   * @param request    the compaction request
   */
  @Override
  public CompactResponse requestCompaction(RpcController controller,
    CompactionProtos.CompactRequest request) throws ServiceException {
    compactionServer.requestCount.increment();
    ServerName rsServerName = ProtobufUtil.toServerName(request.getServer());
    RegionInfo regionInfo = ProtobufUtil.toRegionInfo(request.getRegionInfo());
    ColumnFamilyDescriptor cfd = ProtobufUtil.toColumnFamilyDescriptor(request.getFamily());
    boolean major = request.getMajor();
    int priority = request.getPriority();
    LOG.info("Receive compaction request from {}", ProtobufUtil.toString(request));
    CompactionTask compactionTask = CompactionTask.newBuilder().setRsServerName(rsServerName)
      .setRegionInfo(regionInfo).setColumnFamilyDescriptor(cfd).setRequestMajor(major)
      .setPriority(priority).setFavoredNodes(request.getFavoredNodesList())
      .setSubmitTime(System.currentTimeMillis()).build();
    try {
      compactionServer.compactionThreadManager.requestCompaction(compactionTask);
      return CompactionProtos.CompactResponse.newBuilder().build();
    } catch (IOException ioe) {
      throw new ServiceException(ioe);
    }
  }
}
