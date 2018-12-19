from queue import Queue
from time import sleep

import rclpy
from rclpy.client import Client
from rclpy.executors import MultiThreadedExecutor
from rclpy.node import Node
from rclpy.subscription import Subscription
from std_msgs.msg import String
from threading import Thread


class RclpySpinner(Thread):

    def __init__(self, node):
        super().__init__()
        self._node = node
        self._abort = False
        self._listener_queue = Queue()

    def run(self):
        print('Start called on RclpySpinner, spinning ros2 node')
        executor = MultiThreadedExecutor()
        executor.add_node(self._node)
        while rclpy.ok() and not self._abort:
            executor.spin_once(timeout_sec=1.0)

            # Remove subscribers and clients that have been registered for destruction
            while not self._listener_queue.empty():
                listener = self._listener_queue.get()
                self._node.destroy_subscription(listener)

        if not self._abort:
            print('rclpy.shutdown() was called before QThread.quit()')

    def quit(self):  # noqa: A003
        print('Quit called on RclpySpinner')
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
    sleep(1)  # Crashes
    node.destroy_node()
    rclpy.shutdown()


def dont_segfault(args=None):
    rclpy.init(args=args)

    node = Listener()
    sleep(1)
    node.spinner.register_listeners_for_destruction(node.sub)  # Does not crash
    sleep(1)
    node.destroy_node()
    rclpy.shutdown()


if __name__ == '__main__':
    #dont_segfault()
    segfault()
