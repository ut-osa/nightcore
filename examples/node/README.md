Example of functions implemented in Node.js
==================================

In this example, we define two functions `Foo` and `Bar` in `func_config.json`.
Both functions are implemented in Node.js (`main.js` includes handlers of both functions).
Foo will invoke Bar with Nightcore's API for internal function calls.

This example requires Node.js v12.x to run (we have tested Node.js v12.18.4).
First run `npm install` to install dependencies, which also compiles Nightcore's Node.js module.
Then run `run_stack.sh` to start all Nightcore components.

Finally, run
```
curl -X POST -d "Hello" http://127.0.0.1:8080/function/Foo
```
to invoke function Foo, and you will see the output
```
From function Bar: Hello, World
```
