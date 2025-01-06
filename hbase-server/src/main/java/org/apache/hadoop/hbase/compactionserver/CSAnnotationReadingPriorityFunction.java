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

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ipc.AnnotationReadingPriorityFunction;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.Message;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos;

@InterfaceAudience.Private
public class CSAnnotationReadingPriorityFunction
  extends AnnotationReadingPriorityFunction<CSRpcServices> {
  /**
   * Constructs the priority function given the RPC server implementation and the annotations on the
   * methods.
   * @param rpcServices The RPC server implementation
   */
  public CSAnnotationReadingPriorityFunction(CSRpcServices rpcServices) {
    super(rpcServices);
  }

  @Override
  protected int normalizePriority(int priority) {
    return HConstants.NORMAL_QOS;
  }

  @Override
  protected int getBasePriority(RPCProtos.RequestHeader header, Message param) {
    return HConstants.NORMAL_QOS;
  }

  @Override
  public long getDeadline(RPCProtos.RequestHeader header, Message param) {
    return HConstants.NORMAL_QOS;
  }
}
