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


#include <dirent.h>
#include <gflags/gflags.h>
#include "butil/atomicops.h"
#include "butil/logging.h"
#include "butil/time.h"
#include "brpc/server.h"
#include "bvar/variable.h"
#include "test.pb.h"

DEFINE_int32(port, 8002, "TCP Port of this server");
DEFINE_bool(use_rdma, true, "Use RDMA or not");

butil::atomic<uint64_t> g_last_time(0);
butil::atomic<int> g_use_rdma_transport(0);

namespace {

bool HasRdmaDevice() {
    DIR* dir = opendir("/sys/class/infiniband");
    if (!dir) {
        return false;
    }
    bool has_device = false;
    struct dirent* entry = NULL;
    while ((entry = readdir(dir)) != NULL) {
        if (entry->d_name[0] == '.') {
            continue;
        }
        has_device = true;
        break;
    }
    closedir(dir);
    return has_device;
}

bool ShouldTryRdma() {
    if (!FLAGS_use_rdma) {
        return false;
    }
    if (!HasRdmaDevice()) {
        LOG(WARNING) << "No RDMA device detected, falling back to TCP.";
        return false;
    }
    return true;
}

}  // namespace

namespace test {
class PerfTestServiceImpl : public PerfTestService {
public:
    PerfTestServiceImpl() {}
    ~PerfTestServiceImpl() {}

    void Test(google::protobuf::RpcController* cntl_base,
              const PerfTestRequest* request,
              PerfTestResponse* response,
              google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        uint64_t last = g_last_time.load(butil::memory_order_relaxed);
        uint64_t now = butil::monotonic_time_us();
        if (now > last && now - last > 100000) {
            if (g_last_time.exchange(now, butil::memory_order_relaxed) == last) {
                response->set_cpu_usage(bvar::Variable::describe_exposed("process_cpu_usage"));
            } else {
                response->set_cpu_usage("");
            }
        } else {
            response->set_cpu_usage("");
        }
        if (request->echo_attachment()) {
            brpc::Controller* cntl =
                static_cast<brpc::Controller*>(cntl_base);
            cntl->response_attachment().append(cntl->request_attachment());
        }
    }
};
}

int main(int argc, char* argv[]) {
    GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
    g_use_rdma_transport.store(ShouldTryRdma() ? 1 : 0, butil::memory_order_relaxed);

    brpc::Server server;
    test::PerfTestServiceImpl perf_test_service_impl;

    if (server.AddService(&perf_test_service_impl, 
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }
    g_last_time.store(0, butil::memory_order_relaxed);

    brpc::ServerOptions options;
    options.socket_mode = g_use_rdma_transport.load(butil::memory_order_relaxed) ?
        brpc::SOCKET_MODE_RDMA : brpc::SOCKET_MODE_TCP;
    if (server.Start(FLAGS_port, &options) != 0) {
        if (options.socket_mode == brpc::SOCKET_MODE_RDMA) {
            g_use_rdma_transport.store(0, butil::memory_order_relaxed);
            LOG(WARNING) << "Fail to start server with RDMA, fallback to TCP.";
            options.socket_mode = brpc::SOCKET_MODE_TCP;
            if (server.Start(FLAGS_port, &options) != 0) {
                LOG(ERROR) << "Fail to start server with TCP after fallback";
                return -1;
            }
        } else {
            LOG(ERROR) << "Fail to start EchoServer";
            return -1;
        }
    }

    LOG(INFO) << "rdma_performance server started in "
              << (g_use_rdma_transport.load(butil::memory_order_relaxed) ? "RDMA" : "TCP")
              << " mode.";
    server.RunUntilAskedToQuit();
    return 0;
}
