import sys
import faas

def handler_factory(func_name):
    async def foo_handler(ctx, input_):
        bar_output = await ctx.invoke_func('Bar', input_)
        return 'From function Bar: '.encode() + bar_output

    def bar_handler(ctx, input_):
        return input_ + ', world\n'.encode()
    
    if func_name == 'Foo':
        return foo_handler
    elif func_name == 'Bar':
        return bar_handler
    else:
        sys.stderr.write('Unknown function: {}\n'.format(func_name))
        sys.exit(1)

if __name__ == '__main__':
    faas.serve_forever(handler_factory)
