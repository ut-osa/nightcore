Example of functions implemented in C
==================================

In this example, we define two functions `Foo` and `Bar` in `func_config.json`.
Both functions are implemented in C (`foo.c` and `bar.c`).
Foo will invoke Bar with Nightcore's API for internal function calls.

Run `compile.sh` to compile `.so` files of Foo and Bar, which will be loaded by Nightcore runtime.
Then run `run_stack.sh` to start all Nightcore components.

Finally, run
```
curl -X POST -d "Hello" http://127.0.0.1:8080/function/Foo
```
to invoke function Foo. Then you will see the output
```
From function Bar: Hello, World
```
