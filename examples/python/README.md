Example of functions implemented in Python
==================================

In this example, we define two functions `Foo` and `Bar` in `func_config.json`.
Both functions are implemented in Python (`main.py` includes handlers of both functions).
Foo will invoke Bar with Nightcore's API for internal function calls.

This example requires Python 3.7+ to run (we have tested Python 3.8.5).
First run `make` within `$REPO_ROOT/worker/python` to build Nightcore's Python module.

Then run `run_stack.sh` to start all Nightcore components. Finally, run
```
curl -X POST -d "Hello" http://127.0.0.1:8080/function/Foo
```
to invoke function Foo, and you will see the output
```
From function Bar: Hello, World
```
