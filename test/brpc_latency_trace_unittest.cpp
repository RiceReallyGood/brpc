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

#include <gtest/gtest.h>
#include <type_traits>
#include "brpc/latency_trace.h"

namespace {

TEST(LatencyTraceTest, PointEnumHasExactly36Members) {
    ASSERT_EQ(36, brpc::LT_POINT_COUNT);
    // 客户端 19 个，服务端 17 个，且两段不重叠。
    ASSERT_EQ(0,  brpc::LT_C_RPC_START);
    ASSERT_EQ(18, brpc::LT_C_RPC_END);
    ASSERT_EQ(19, brpc::LT_S_WAKE);
    ASSERT_EQ(35, brpc::LT_S_WRITE_END);
}

TEST(LatencyTraceTest, RecordIsExactly200Bytes) {
    // 定长 POD。大小变化会直接改变 dump 文件格式，必须显式感知。
    ASSERT_EQ(200u, sizeof(brpc::LatencyTraceRecord));
    ASSERT_TRUE(std::is_trivially_copyable<brpc::LatencyTraceRecord>::value);
}

}  // namespace
