Nightcore
==================================

Nightcore is a research function-as-a-service (FaaS) runtime with μs-scale latency and high throughput.
Nightcore targets stateless microservices that requires low latency.
The current prototype supports functions written in C/C++, Go, Node.js, and Python.

### Building Nightcore ###

```
./build_deps.sh
make -j $(nproc)
```

### Running "Hello world" examples ###

Inside [examples](https://github.com/ut-osa/nightcore/tree/asplos-release/examples) folder,
we provide "Hello world" example functions implemented in all supported programming languages.
These functions demonstrate the basic usage of Nightcore's API for internal function calls.

### Running microservice workloads ###

A separate repository [ut-osa/nightcore-benchmarks](https://github.com/ut-osa/nightcore-benchmarks)
includes scripts and detailed instructions on running microservice workloads presented in our ASPLOS paper.
