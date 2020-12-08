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

TODO

### Running microservice workloads ###

Another repository [ut-osa/nightcore-benchmarks](https://github.com/ut-osa/nightcore-benchmarks)
includes scripts and detailed instructions on running microservice workloads presented in our ASPLOS paper.
