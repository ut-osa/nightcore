Example of functions implemented in Go
==================================

In this example, we define two functions `Foo` and `Bar` in `func_config.json`.
Both functions are implemented in Go (`main.go` includes handlers of both functions).
Foo will invoke Bar with Nightcore's API for internal function calls.

This example requires Go 1.14.x to compile (we have tested Go 1.14.12):
```
go build -o main
```
Then run `run_stack.sh` to start all Nightcore components.

Finally, run
```
curl -X POST -d "Hello" http://127.0.0.1:8080/function/Foo
```
to invoke function Foo, and you will see the output
```
From function Bar: Hello, World
```
