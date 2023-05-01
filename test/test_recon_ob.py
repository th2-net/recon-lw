import unittest
from recon_lw.recon_ob import *
from sortedcontainers import SortedKeyList


class ReconObTests(unittest.TestCase):
    def test_sequence_cache_add(self):
        sequence_cache = {'sequence': SortedKeyList(), 'times': SortedKeyList(), 'duplicates': SortedKeyList()}
        # Add first message
        seq = 1
        timestamp = {'epochSecond': 1681691163, 'nano': 1999906000}
        message1 = {'messageId': 'test:001'}
        sequence_cache_add(seq, timestamp, message1, sequence_cache)
        self.assertEqual(sequence_cache['sequence'][0], (seq, message1), "Fail to add first message in sequence cache")
        self.assertEqual(sequence_cache['times'][0], (timestamp, seq), "Fail to add first timestamp in sequence cache")
        # Add duplicate
        seq = 1
        timestamp = {'epochSecond': 1681691164, 'nano': 1999906000}
        message2 = {'messageId': 'test:002'}
        sequence_cache_add(seq, timestamp, message2, sequence_cache)
        self.assertEqual(sequence_cache['duplicates'][0], (seq, message2['messageId'], message1['messageId']),
                         "Fail to process duplicate")
        self.assertEqual(len(sequence_cache['sequence']), 1, "Fail to process duplicate")
        self.assertEqual(len(sequence_cache['times']), 1, "Fail to process duplicate")
        # Add second message
        seq = 2
        timestamp = {'epochSecond': 1681691165, 'nano': 1999906000}
        message3 = {'messageId': 'test:003'}
        sequence_cache_add(seq, timestamp, message3, sequence_cache)
        self.assertEqual(sequence_cache['sequence'][1], (seq, message3), "Fail to add second message in sequence cache")
        self.assertEqual(sequence_cache['times'][1], (timestamp, seq), "Fail to add second timestamp in sequence cache")
        # Add gap
        seq = 4
        timestamp = {'epochSecond': 1681691175, 'nano': 1999906000}
        message4 = {'messageId': 'test:004'}
        sequence_cache_add(seq, timestamp, message4, sequence_cache)
        self.assertEqual(sequence_cache['sequence'][2], (3, {'gap': True}), "Fail to process gap")
        self.assertEqual(sequence_cache['sequence'][3], (seq, message4), "Fail to process gap")
        self.assertEqual(sequence_cache['times'][2], (timestamp, seq), "Fail to process gap")

    def test_ob_add_order(self):
        order_book = {'ask': {}, 'bid': {}, 'status': '?', 'aggr_max_levels': 15}
        # Add new order
        ord_id = 'id1'
        price = 1.5
        size = 10
        side = 'bid'
        err, ob = ob_add_order(ord_id, price, size, side, order_book)
        self.assertEqual(err, {}, "Failed to add new order")
        self.assertEqual(order_book[side][price][ord_id], size, "Failed to add new order")
        self.assertEqual(ob[0], order_book, "Failed to add new order")
        # Add again
        err, ob = ob_add_order(ord_id, price, size, side, order_book)
        self.assertEqual(err, {'error': ord_id + " already exists"}, "Unable to process dup error")
        self.assertEqual(ob, [], "Unable to process dup error")
        # Add new order with same price
        ord_id = 'id2'
        price = 1.5
        size = 100
        side = 'bid'
        err, ob = ob_add_order(ord_id, price, size, side, order_book)
        self.assertEqual(err, {}, "Failed to add new order")
        self.assertEqual(len(order_book[side][price]), 2, "Failed to add new order")
        self.assertEqual(order_book[side][price][ord_id], size, "Failed to add new order")
        self.assertEqual(ob[0], order_book, "Failed to add new order")
        # Add new order with diff price
        ord_id = 'id3'
        price = 1.6
        size = 100
        side = 'bid'
        err, ob = ob_add_order(ord_id, price, size, side, order_book)
        self.assertEqual(err, {}, "Failed to add new order")
        self.assertEqual(len(order_book[side]), 2, "Failed to add new order")
        self.assertEqual(order_book[side][price][ord_id], size, "Failed to add new order")
        self.assertEqual(ob[0], order_book, "Failed to add new order")