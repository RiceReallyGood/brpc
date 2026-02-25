# rdma_performance

This example now supports automatic fallback to TCP, so it can run even when
the environment does not support RDMA.

## Build

### Make

```bash
cd example/rdma_performance
make
```

### CMake

```bash
cd example/rdma_performance
cmake -B build-gcc -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++
cmake --build build-gcc -j
```

## Run and test performance (no RDMA environment)

Start server (the example will fallback to TCP automatically if RDMA is not
available):

```bash
./server --port=8002
```

Run client in another terminal:

```bash
./client \
  --servers=127.0.0.1:8002+127.0.0.1:8002 \
  --test_seconds=10 \
  --thread_num=4 \
  --queue_depth=4 \
  --attachment_size=1024
```

The client prints Avg/percentile latency, throughput and QPS.

## Force TCP mode explicitly

```bash
./server --use_rdma=false --port=8002
./client --use_rdma=false --servers=127.0.0.1:8002+127.0.0.1:8002
```

