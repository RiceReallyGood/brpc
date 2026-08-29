#include <gtest/gtest.h>
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
