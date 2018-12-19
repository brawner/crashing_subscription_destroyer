from queue import Queue
from time import sleep

from PyQt5.QtCore import qDebug, QThread, qWarning
import rclpy
from rclpy.client import Client
from rclpy.executors import MultiThreadedExecutor
from rclpy.node import Node
from rclpy.subscription import Subscription
from std_msgs.msg import String


class RclpySpinner(QThread):

    def __init__(self, node):
        super().__init__()
        self._node = node
        self._abort = False
        self._listener_queue = Queue()

    def run(self):
        qDebug('Start called on RclpySpinner, spinning ros2 node')
        executor = MultiThreadedExecutor()
        executor.add_node(self._node)
        while rclpy.ok() and not self._abort:
            executor.spin_once(timeout_sec=1.0)

            # Remove subscribers and clients that have been registered for destruction
            while not self._listener_queue.empty():
                listener = self._listener_queue.get()
                self._node.destroy_subscription(listener)

        if not self._abort:
            qWarning('rclpy.shutdown() was called before QThread.quit()')

    def quit(self):  # noqa: A003
        qDebug('Quit called on RclpySpinner')
        self._abort = True
        super().quit()

    def register_listeners_for_destruction(self, *listeners):
        for listener in listeners:
            self._listener_queue.put(listener)


class Listener(Node):

    def __init__(self):
        super().__init__('listener')
        self.spinner = RclpySpinner(self)
        self.sub = self.create_subscription(String, 'chatter', self.chatter_callback)
        self.spinner.start()

    def chatter_callback(self, msg):
        self.get_logger().info('I heard: [%s]' % msg.data)


def segfault(args=None):
    rclpy.init(args=args)

    node = Listener()
    sleep(1)
    node.destroy_subscription(node.sub)
    sleep(1)
    node.destroy_node()  # Crashes
    rclpy.shutdown()


def dont_segfault(args=None):
    rclpy.init(args=args)

    node = Listener()
    sleep(1)
    node.spinner.register_listeners_for_destruction(node.sub)  # Does not crash
    sleep(1)
    node.destroy_node()  # Crashes
    rclpy.shutdown()


if __name__ == '__main__':
    #dont_segfault()
    segfault()

    Thread 6 Crashed:: RclpySpinner
0   librmw_fastrtps_shared_cpp.dylib	0x0000000104de81ae SubListener::hasData() + 126
1   librmw_fastrtps_shared_cpp.dylib	0x0000000104de7f43 check_wait_set_for_data(rmw_subscriptions_t const*, rmw_guard_conditions_t const*, rmw_services_t const*, rmw_clients_t const*) + 115
2   librmw_fastrtps_shared_cpp.dylib	0x0000000104de97c7 rmw_fastrtps_shared_cpp::__rmw_wait(rmw_subscriptions_t*, rmw_guard_conditions_t*, rmw_services_t*, rmw_clients_t*, rmw_wait_set_t*, rmw_time_t const*)::$_0::operator()() const + 39
3   librmw_fastrtps_shared_cpp.dylib	0x0000000104de983f bool std::__1::condition_variable::wait_until<std::__1::chrono::steady_clock, std::__1::chrono::duration<long long, std::__1::ratio<1l, 1000000000l> >, rmw_fastrtps_shared_cpp::__rmw_wait(rmw_subscriptions_t*, rmw_guard_conditions_t*, rmw_services_t*, rmw_clients_t*, rmw_wait_set_t*, rmw_time_t const*)::$_0>(std::__1::unique_lock<std::__1::mutex>&, std::__1::chrono::time_point<std::__1::chrono::steady_clock, std::__1::chrono::duration<long long, std::__1::ratio<1l, 1000000000l> > > const&, rmw_fastrtps_shared_cpp::__rmw_wait(rmw_subscriptions_t*, rmw_guard_conditions_t*, rmw_services_t*, rmw_clients_t*, rmw_wait_set_t*, rmw_time_t const*)::$_0) + 95
4   librmw_fastrtps_shared_cpp.dylib	0x0000000104de8d81 rmw_fastrtps_shared_cpp::__rmw_wait(rmw_subscriptions_t*, rmw_guard_conditions_t*, rmw_services_t*, rmw_clients_t*, rmw_wait_set_t*, rmw_time_t const*) + 2561
5   librmw_fastrtps_cpp.dylib     	0x0000000104d4558d rmw_wait + 61
6   librmw_implementation.dylib   	0x0000000103a83c74 rmw_wait + 132
7   librcl.dylib                  	0x00000001039651fa rcl_wait + 2410
8   _rclpy.cpython-37m-darwin.so  	0x000000010392eaa8 rclpy_wait + 200
9   org.python.python             	0x0000000102ece20f _PyMethodDef_RawFastCallKeywords + 236
10  org.python.python             	0x0000000102ecd8af _PyCFunction_FastCallKeywords + 44
11  org.python.python             	0x0000000102f63b2b call_function + 636
12  org.python.python             	0x0000000102f5c771 _PyEval_EvalFrameDefault + 7016
13  org.python.python             	0x0000000102ed94bf gen_send_ex + 242
14  org.python.python             	0x0000000102f59111 builtin_next + 99
15  org.python.python             	0x0000000102ece313 _PyMethodDef_RawFastCallKeywords + 496
16  org.python.python             	0x0000000102ecd8af _PyCFunction_FastCallKeywords + 44
17  org.python.python             	0x0000000102f63b2b call_function + 636
18  org.python.python             	0x0000000102f5c80f _PyEval_EvalFrameDefault + 7174
19  org.python.python             	0x0000000102f64432 _PyEval_EvalCodeWithName + 1835
20  org.python.python             	0x0000000102ecd874 _PyFunction_FastCallKeywords + 225
21  org.python.python             	0x0000000102f63ba0 call_function + 753
22  org.python.python             	0x0000000102f5c8b5 _PyEval_EvalFrameDefault + 7340
23  org.python.python             	0x0000000102f64432 _PyEval_EvalCodeWithName + 1835
24  org.python.python             	0x0000000102ecd874 _PyFunction_FastCallKeywords + 225
25  org.python.python             	0x0000000102f63ba0 call_function + 753
26  org.python.python             	0x0000000102f5c8b5 _PyEval_EvalFrameDefault + 7340
27  org.python.python             	0x0000000102ecdc8a function_code_fastcall + 112
28  org.python.python             	0x0000000102ece60d _PyObject_Call_Prepend + 150
29  org.python.python             	0x0000000102ecd9bd PyObject_Call + 136
30  sip.so                        	0x0000000104b72122 call_method + 76
31  sip.so                        	0x0000000104b6df78 sip_api_call_procedure_method + 152
32  QtCore.so                     	0x00000001043cc2e4 sipQThread::run() + 84
33  org.qt-project.QtCore         	0x00000001045d275e 0x1045a4000 + 190302
34  libsystem_pthread.dylib       	0x00007fff6fb5a661 _pthread_body + 340
35  libsystem_pthread.dylib       	0x00007fff6fb5a50d _pthread_start + 377
36  libsystem_pthread.dylib       	0x00007fff6fb59bf9 thread_start + 13
