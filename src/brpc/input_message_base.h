// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#ifndef BRPC_INPUT_MESSAGE_BASE_H
#define BRPC_INPUT_MESSAGE_BASE_H

#include "brpc/socket_id.h"           // SocketId
#include "brpc/destroyable.h"         // DestroyingPtr
#if defined(BRPC_LATENCY_TRACE)
#include <stdint.h>                   // uint64_t
#endif


namespace brpc {

// Messages returned by Parse handlers must extend this class
class InputMessageBase : public Destroyable {
protected:
    // Implement this method to customize deletion of this message.
    virtual void DestroyImpl() = 0;
    
public:
    // Called to release the memory of this message instead of "delete"
    void Destroy();
    
    // Own the socket where this message is from.
    Socket* ReleaseSocket();

    // Get the socket where this message is from.
    Socket* socket() const { return _socket.get(); }

    // Arg of the InputMessageHandler which parses this message successfully.
    const void* arg() const { return _arg; }

    // [Internal]
    int64_t received_us() const { return _received_us; }
    int64_t base_real_us() const { return _base_real_us; }

protected:
    virtual ~InputMessageBase();

private:
friend class InputMessenger;
friend void* ProcessInputMessage(void*);
friend class Stream;
friend class Transport;
    int64_t _received_us;
    int64_t _base_real_us;
    SocketUniquePtr _socket;
    void (*_process)(InputMessageBase* msg);
    const void* _arg;
#if defined(BRPC_LATENCY_TRACE)
    // Copied from the owning Socket's _lt_wake/_lt_onedge_start/
    // _lt_readv_start at the moment this message was cut out of the read
    // buffer in InputMessenger::ProcessNewMessage, plus the cut-out time
    // itself. The handle needed to write these into a trace record is
    // still unknown at that point (the message hasn't been parsed as an
    // RPC yet), so they ride along here until a protocol handler
    // (e.g. ProcessRpcRequest/ProcessRpcResponse) learns the handle and
    // stamps them. See design doc sec.8.2.
    uint64_t _lt_wake;
    uint64_t _lt_onedge_start;
    uint64_t _lt_readv_start;
    uint64_t _lt_msg_recv_done;
#endif
};

} // namespace brpc


#endif  // BRPC_INPUT_MESSAGE_BASE_H
