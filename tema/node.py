"""
    This module represents a cluster's computational node.

    Computer Systems Architecture course
    Assignment 1 - Cluster Activity Simulation
    march 2013
"""

import threading
from datastore import *
from threading import *
from time import sleep
import random

threading.stack_size(32*1024) 
commLock = Lock()

reg1 = False
reg2 = False

class NodeThread(Thread):
    def __init__(self, node, start_row, start_column, num_rows, num_columns, block, event):
        """
            Constructor

            @param node: the owner node of this thread
            @param start_row: the index of the first row in the block
            @param start_column: the index of the first column in the block
            @param num_rows: number of rows in the block
            @param num_columns: number of columns in the block
            @param block: the block of the result matrix encoded as a row-order list of lists of integers
        """
        Thread.__init__(self)
        self.node = node 
        self.start_row = start_row
        self.start_column = start_column
        self.num_rows = num_rows
        self.num_columns = num_columns
        self.block = block
        self.event = event

    def find_node(self, i, j):
        """
            Finds the node whose datastore contains the element at position (i, j).

            @param i: the row index
            @param j: the column index

            @return: the node described above
        """

        index_row = i / self.node.block_size
        index_col = j / self.node.block_size

        for node in self.node.nodes:
            if (node.node_ID[0] == index_row) & (node.node_ID[1] == index_col):
                retNode = node
                break

        return retNode


    def run(self):
        global commLock

        # Inregistreaza threadul nodului la datastore
        self.node.data_store.register_thread(self.node)

        # Determina blocul din matricea finala
        final_row = []
        final_elem = 0
        # commLock.acquire()
        for i in range(self.start_row, self.start_row  + self.num_rows):
            final_row = []
            for j in range(self.start_column, self.start_column + self.num_columns):
                final_elem = 0
                for k in range(self.node.matrix_size):
                    if ((i / self.node.block_size) == self.node.node_ID[0]) and ((k / self.node.block_size) == self.node.node_ID[1]):
                        elem_a = self.node.data_store.get_element_from_a(self.node, i - (self.node.node_ID[0] * self.node.block_size), \
                                                                        k - (self.node.node_ID[1] * self.node.block_size))
                    else:
                        node_a = self.find_node(i, k)
                        # node_a.lock.acquire()
                        with commLock:#.acquire()
                            node_a.commThread.set_coord(i - (node_a.node_ID[0] * node_a.block_size), k - (node_a.node_ID[1] * node_a.block_size))
                            # while not node_a.commThread.event.isSet():
                                # node_a.commThread.event.wait()
                            # print '1',
                            node_a.commThread.semaphore.acquire()
                            # print '1'
                            elem_a = node_a.commThread.get_elem_from_a()
                            # node_a.commThread.event.clear()
                            # node_a.lock.release()
                        # commLock.release()
                    if ((k / self.node.block_size) == self.node.node_ID[0]) and ((j / self.node.block_size) == self.node.node_ID[1]):
                        elem_b = self.node.data_store.get_element_from_b(self.node, k - (self.node.node_ID[0] * self.node.block_size), \
                                                                        j - (self.node.node_ID[1] * self.node.block_size))
                    else:
                        node_b = self.find_node(k, j)
                        # node_b.lock.acquire()
                        with commLock:#.acquire()
                            node_b.commThread.set_coord(k - (node_b.node_ID[0] * node_b.block_size), j - (node_b.node_ID[1] * node_b.block_size))
                            # while not node_b.commThread.event.isSet():
                                # node_b.commThread.event.wait()
                            node_b.commThread.semaphore.acquire()
                            # print '1'
                            elem_b = node_b.commThread.get_elem_from_b()
                            # node_b.commThread.event.clear()
                            # node_b.lock.release()
                        # commLock.release()
                    final_elem = final_elem + elem_a * elem_b

                final_row.append(final_elem)
            self.block.append(final_row)
        self.event.set()

        # commLock.release()


class CommunicationThread(Thread):
    def __init__(self, node):
        Thread.__init__(self)
        self.node = node
        self.a = 0
        self.b = 0
        self.ic = 0
        self.jc = 0
        self.running = Event()
        self.event = Event()
        self.coord_event = Event()
        self.semaphore = Semaphore(value=0)
        self.lock = Lock()
        self.coord_event.clear()

    def run(self):
        # Inregistreaza threadul nodului la datastore
        self.node.data_store.register_thread(self.node)

        while 1:
            with self.lock:
                if self.running.isSet(): 
                    return

                while not self.coord_event.isSet():
                    self.coord_event.wait()

                if self.running.isSet(): 
                    return
                # print '2'
                self.a = self.node.data_store.get_element_from_a(self.node, self.ic, self.jc)
                self.b = self.node.data_store.get_element_from_b(self.node, self.ic, self.jc)                 
                # self.event.set()
                self.coord_event.clear()
                self.semaphore.release()
                
                if self.running.isSet(): 
                    return

    def get_elem_from_a(self):
        return self.a

    def get_elem_from_b(self):
        return self.b

    def set_coord(self, i, j):
        self.ic = i
        self.jc = j
        self.coord_event.set()

    # def set_running(self, value):
        # self.running = value


class Node():
    """
        Class that represents a cluster node with computation and storage functionalities.
    """

    def __init__(self, node_ID, block_size, matrix_size, data_store):
        """
            Constructor.

            @param node_ID: a pair of IDs uniquely identifying the node; 
            IDs are integers between 0 and matrix_size/block_size
            @param block_size: the size of the matrix blocks stored in this node's datastore
            @param matrix_size: the size of the matrix
            @param data_store: reference to the node's local data store
        """
        global commThreads

        self.node_ID = node_ID
        self.block_size = block_size
        self.matrix_size = matrix_size
        self.data_store = data_store
        self.task_threads = []
        self.lock = Lock()

        self.commThread = CommunicationThread(self)
        self.commThread.start()

    def set_nodes(self, nodes):
        """
            Informs the current node of the other nodes in the cluster. 
            Guaranteed to be called before the first call to compute_matrix_block.

            @param nodes: a list containing all the nodes in the cluster
        """
        self.nodes = nodes

    def compute_matrix_block(self, start_row, start_column, num_rows, num_columns):
        """
            Computes a given block of the result matrix.
            The method invoked by FEP nodes.

            @param start_row: the index of the first row in the block
            @param start_column: the index of the first column in the block
            @param num_rows: number of rows in the block
            @param num_columns: number of columns in the block

            @return: the block of the result matrix encoded as a row-order list of lists of integers
        """
        block = []

        event = Event()
        thread = NodeThread(self, start_row, start_column, num_rows, num_columns, block, event)
        thread.start()
        self.task_threads.append(thread)

        while not event.isSet():
            event.wait()

        return block

    def shutdown(self):
        """
            Instructs the node to shutdown (terminate all threads).
        """
        global threads, commThreads, commLock

        # for thr in commThreads:
            # thr.set_running(False)
        self.commThread.running.set()
        self.commThread.coord_event.set()    
        self.commThread.join()
            # print 'thread incheiat'

        for thr in self.task_threads:
            thr.join()
