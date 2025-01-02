package org.apache.hadoop.hbase.compactionserver;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ipc.AnnotationReadingPriorityFunction;
import org.apache.hadoop.hbase.master.MasterRpcServices;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos;
import org.apache.hbase.thirdparty.com.google.protobuf.Message;

public class CSAnnotationReadingPriorityFunction  extends
  AnnotationReadingPriorityFunction<CSRpcServices> {
  /**
   * Constructs the priority function given the RPC server implementation and the annotations on the
   * methods.
   *
   * @param rpcServices The RPC server implementation
   */
  public CSAnnotationReadingPriorityFunction(CSRpcServices rpcServices) {
    super(rpcServices);
  }

  @Override protected int normalizePriority(int priority) {
    return HConstants.NORMAL_QOS;
  }

  @Override protected int getBasePriority(RPCProtos.RequestHeader header, Message param) {
    return HConstants.NORMAL_QOS;
  }

  @Override public long getDeadline(RPCProtos.RequestHeader header, Message param) {
    return HConstants.NORMAL_QOS;
  }
}
