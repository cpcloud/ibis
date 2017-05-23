from multipledispatch import Dispatcher


execute = Dispatcher('execute')
execute_node = Dispatcher('execute_node')
execute_direct = Dispatcher('execute_direct')
