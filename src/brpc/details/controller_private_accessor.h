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

#ifndef BRPC_CONTROLLER_PRIVATE_ACCESSOR_H
#define BRPC_CONTROLLER_PRIVATE_ACCESSOR_H

// This is an rpc-internal file.

#include "brpc/socket.h"
#include "brpc/controller.h"
#include "brpc/stream.h"
#include "brpc/latency_trace.h"

namespace google {
namespace protobuf {
class Message;
}
}

namespace brpc {

class Span;

class AuthContext;

// A wrapper to access some private methods/fields of `Controller'
// This is supposed to be used by internal RPC protocols ONLY
class ControllerPrivateAccessor {
public:
    explicit ControllerPrivateAccessor(Controller* cntl) {
        _cntl = cntl;
    }

    void OnResponse(CallId id, int saved_error) {
        const Controller::CompletionInfo info = { id, true };
        _cntl->OnVersionedRPCReturned(info, false, saved_error);
    }

#if defined(BRPC_LATENCY_TRACE)
    void set_latency_trace(uint64_t trace_id, LatencyTraceHandle h) {
        _cntl->_lt_trace_id = trace_id;
        _cntl->_lt_handle = h;
        // Mirror onto _current_call too (fix-round item 2): on the
        // client, this is Channel::CallMethod allocating the FIRST
        // attempt's slot, and _current_call IS that first attempt --
        // Call::OnComplete()/a later backup request both need this
        // attempt's own handle available on the Call itself, not only on
        // the Controller. Harmless on the server side (this same setter
        // also runs there, from baidu_rpc_protocol.cpp's
        // ProcessRpcRequest): a server-side Controller never retries, so
        // _current_call.lt_handle just sits unread there.
        _cntl->_current_call.lt_handle = h;
    }

    uint64_t latency_trace_id() const {
        return _cntl->_lt_trace_id;
    }

    LatencyTraceHandle latency_trace_handle() const {
        return _cntl->_lt_handle;
    }

    // Fix-round item 1: resolve the handle for the Call that actually
    // produced a given wire response, instead of `_cntl->_lt_handle`
    // (which only gets repointed at the winning attempt later, inside
    // EndRPC -> Call::OnComplete(end_of_rpc=true) -- see fix-round item 2's
    // comment on that function). ProcessRpcResponse runs BEFORE that
    // repoint, so reading `_lt_handle` there names whichever attempt
    // IssueRPC allocated most recently, not necessarily the one whose
    // response this is: with backup requests, "original wins over its own
    // backup" is exactly the case where those two differ (this response is
    // for the original, `_lt_handle` already points at the backup that
    // IssueRPC allocated after it).
    //
    // `Call::id()` is `_correlation_id.value + nretry + 1` (see
    // Controller::get_id()), and that id is what goes out on the wire and
    // comes back as `meta.correlation_id()` -- so subtracting it back out
    // gives the attempt index without needing any new wire field. Walk
    // `_current_call` and `_unfinished_call` (there are never more than
    // these two live attempts at once, see the class comment on
    // `_unfinished_call`) and return whichever one's `nretry` matches.
    LatencyTraceHandle latency_trace_handle_for_response(
            int64_t correlation_id) const {
        const int nretry =
            (int)(correlation_id - _cntl->_correlation_id.value - 1);
        if (_cntl->_current_call.nretry == nretry) {
            return _cntl->_current_call.lt_handle;
        }
        if (_cntl->_unfinished_call != nullptr &&
            _cntl->_unfinished_call->nretry == nretry) {
            return _cntl->_unfinished_call->lt_handle;
        }
        // No live attempt matches -- an ordinary case, not an exotic one:
        // bthread/id.cpp's ranged id accepts any version, so a superseded
        // attempt's late response can still reach ProcessRpcResponse after
        // its Call has been destroyed (see the fix-round item 2 report;
        // brpc's own comment near controller.cpp:1535 describes exactly
        // this). Returning `_cntl->_lt_handle` here would be wrong, not
        // merely imprecise: with `_stop_when_full` (the only supported
        // production setting) slots are never recycled, so the generation
        // guard does NOT refuse this write -- it would silently land this
        // stale attempt's receive-side timestamps on whatever LIVE record
        // `_lt_handle` currently points at (e.g. a retry/backup attempt
        // already in flight), corrupting it with first-write-wins values
        // that make no sense for that attempt. Dropping the stamp for an
        // attempt whose Call is gone is correct; polluting a live one is
        // not.
        return LT_INVALID_HANDLE;
    }
#else
    void set_latency_trace(uint64_t /*trace_id*/, LatencyTraceHandle /*h*/) {}
    uint64_t latency_trace_id() const { return 0; }
    LatencyTraceHandle latency_trace_handle() const { return LT_INVALID_HANDLE; }
    LatencyTraceHandle latency_trace_handle_for_response(int64_t) const {
        return LT_INVALID_HANDLE;
    }
#endif

    ControllerPrivateAccessor &set_peer_id(SocketId peer_id) {
        _cntl->_current_call.peer_id = peer_id;
        return *this;
    }

    Socket* get_sending_socket() {
        return _cntl->_current_call.sending_sock.get();
    }

    int64_t real_timeout_ms() {
        return _cntl->_real_timeout_ms;
    }

    void move_in_server_receiving_sock(SocketUniquePtr& ptr) {
        CHECK(_cntl->_current_call.sending_sock == nullptr);
        _cntl->_current_call.sending_sock.reset(ptr.release());
    }

    StreamUserData* get_stream_user_data() {
        return _cntl->_current_call.stream_user_data;
    }

    ControllerPrivateAccessor &set_security_mode(bool security_mode) {
        _cntl->set_flag(Controller::FLAGS_SECURITY_MODE, security_mode);
        return *this;
    }

    ControllerPrivateAccessor &set_remote_side(const butil::EndPoint& pt) {
        _cntl->_remote_side = pt;
        return *this;
    }

    ControllerPrivateAccessor &set_local_side(const butil::EndPoint& pt) {
        _cntl->_local_side = pt;
        return *this;
    }
 
    ControllerPrivateAccessor &set_auth_context(const AuthContext* ctx) {
        _cntl->set_auth_context(ctx);
        return *this;
    }

    // Overloaded set_span methods to support both shared_ptr and raw pointer
    ControllerPrivateAccessor &set_span(const std::shared_ptr<Span>& span);
    ControllerPrivateAccessor &set_span(Span* span);
    
    ControllerPrivateAccessor &set_request_protocol(ProtocolType protocol) {
        _cntl->_request_protocol = protocol;
        return *this;
    }
    
    std::shared_ptr<Span> span() const;

    uint32_t pipelined_count() const { return _cntl->_pipelined_count; }
    void set_pipelined_count(uint32_t count) {  _cntl->_pipelined_count = count; }

    // The mysql protocol stores its statement type (MYSQL_NORMAL_STATEMENT /
    // MYSQL_PREPARED_STATEMENT) in the pipelined_count slot.
    void set_mysql_statement_type(uint32_t type) { set_pipelined_count(type); }

    ControllerPrivateAccessor& set_server(const Server* server) {
        _cntl->_server = server;
        return *this;
    }

    // Pass the owership of |settings| to _cntl, while is going to be
    // destroyed in Controller::Reset()
    void set_remote_stream_settings(StreamSettings *settings) {
        _cntl->_remote_stream_settings = settings;
    }
    StreamSettings* remote_stream_settings() {
        return _cntl->_remote_stream_settings;
    }

    StreamIds request_streams() { return _cntl->_request_streams; }
    StreamIds response_streams() { return _cntl->_response_streams; }

    void set_method(const google::protobuf::MethodDescriptor* method) 
    { _cntl->_method = method; }

    void set_readable_progressive_attachment(ReadableProgressiveAttachment* s)
    { _cntl->_rpa.reset(s); }

    void set_readable_progressive_attachment(
        ReadableProgressiveAttachment* s, SocketId socket_id) {
        _cntl->_rpa.reset(s);
        _cntl->_progressive_read_socket_id = socket_id;
    }

    void set_auth_flags(uint32_t auth_flags) {
        _cntl->_auth_flags = auth_flags;
    }

    void clear_auth_flags() { _cntl->_auth_flags = 0; }

    // Set how the sending socket is reserved after the RPC (mysql transactions).
    void set_bind_sock_action(BindSockAction action) { _cntl->set_bind_sock_action(action); }
    // Transfer ownership of the reserved socket to `ptr`.
    void get_bind_sock(SocketUniquePtr* ptr) {
        if (_cntl->_bind_sock) {
            _cntl->_bind_sock->ReAddress(ptr);
        }
    }
    // Reuse an externally-reserved socket for the next RPC.
    void use_bind_sock(SocketId sock_id) {
        _cntl->set_bind_sock_action(BIND_SOCK_USE);
        Socket::Address(sock_id, &_cntl->_bind_sock);
    }
    void set_session_data(void* d) { _cntl->_session_data = d; }
    void* session_data() const { return _cntl->_session_data; }

    std::string& protocol_param() { return _cntl->protocol_param(); }
    const std::string& protocol_param() const { return _cntl->protocol_param(); }

    // Note: This function can only be called in server side. The deadline of client
    // side is properly set in the RPC sending path.
    void set_deadline_us(int64_t deadline_us) { _cntl->_deadline_us = deadline_us; }

    ControllerPrivateAccessor& set_begin_time_us(int64_t begin_time_us) {
        _cntl->_begin_time_us = begin_time_us;
        _cntl->_end_time_us = UNSET_MAGIC_NUM;
        return *this;
    }

    ControllerPrivateAccessor& set_health_check_call() {
        _cntl->add_flag(Controller::FLAGS_HEALTH_CHECK_CALL);
        return *this;
    }

    void set_checksum_value(const char* c, size_t size) {
        _cntl->_checksum_value.assign(c, size);
    }

    void set_checksum_value(const std::string& c) {
        _cntl->_checksum_value = c;
    }

    const std::string& checksum_value() const { return _cntl->_checksum_value; }

private:
    Controller* _cntl;
};

// Inherit this class to intercept Controller::IssueRPC. This is an internal
// utility only useable by brpc developers.
class RPCSender {
public:
    virtual ~RPCSender() {}
    virtual int IssueRPC(int64_t start_realtime_us) = 0;
};

} // namespace brpc


#endif // BRPC_CONTROLLER_PRIVATE_ACCESSOR_H
