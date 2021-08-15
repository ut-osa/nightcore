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

### Publication ###

Please cite our [ASPLOS '21 paper](https://dl.acm.org/doi/abs/10.1145/3445814.3446701) if you use Nightcore in your project:

~~~
@inproceedings{jia2021nightcore,
author = {Jia, Zhipeng and Witchel, Emmett},
title = {Nightcore: Efficient and Scalable Serverless Computing for Latency-Sensitive, Interactive Microservices},
year = {2021},
isbn = {9781450383172},
publisher = {Association for Computing Machinery},
address = {New York, NY, USA},
url = {https://doi.org/10.1145/3445814.3446701},
doi = {10.1145/3445814.3446701},
booktitle = {Proceedings of the 26th ACM International Conference on Architectural Support for Programming Languages and Operating Systems},
pages = {152–166},
numpages = {15},
keywords = {function-as-a-service, Cloud computing, serverless computing, microservices},
location = {Virtual, USA},
series = {ASPLOS 2021}
}
~~~
