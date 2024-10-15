import unittest
from recon_lw.matching.recon_ob import *
from sortedcontainers import SortedKeyList


class ReconObTests(unittest.TestCase):
    def test_sequence_cache_add(self):
        sequence_cache = SequenceCache(5)
        # Add first message
        seq = 1
        timestamp = {"epochSecond": 1681691163, "nano": 1999906000}
        message1 = {"messageId": "test:001"}
        sequence_cache.add_object(seq, timestamp, message1)
        self.assertEqual(
            sequence_cache._objects[seq],
            message1,
            "Fail to add first message in sequence cache",
        )

        self.assertEqual(
            sequence_cache._time_indexes[timestamp['epochSecond']],
            [seq, seq],
            "Fail to add first timestamp in sequence cache",
        )
        # Add duplicate
        seq = 1
        timestamp = {"epochSecond": 1681691164, "nano": 1999906000}
        message2 = {"messageId": "test:002"}
        sequence_cache.add_object(seq, timestamp, message2)
        self.assertEqual(
            sequence_cache.get_duplicates_collection()[0],
            (seq, message2, message1),
            "Fail to process duplicate",
        )
        self.assertEqual(len(sequence_cache._objects), 1, "Fail to process duplicate")
        self.assertEqual(len(sequence_cache._time_indexes), 1, "Fail to process duplicate")
        # Add second message
        seq = 2
        timestamp = {"epochSecond": 1681691165, "nano": 1999906000}
        last_ts = timestamp
        message3 = {"messageId": "test:003"}
        sequence_cache.add_object(seq, timestamp, message3)
        self.assertEqual(
            sequence_cache._objects[seq],
            message3,
            "Fail to add second message in sequence cache",
        )
        self.assertEqual(
            sequence_cache._time_indexes[timestamp['epochSecond']],
            [seq, seq],
            "Fail to add second timestamp in sequence cache",
        )
        # Add gap
        seq = 4
        timestamp = {"epochSecond": 1681691175, "nano": 1999906000}
        message4 = {"messageId": "test:004"}
        sequence_cache.add_object(seq, timestamp, message4)

        self.assertEqual(sequence_cache._gaps[0], {'s1': 3, 't1': last_ts, 's2': 3, 't2': timestamp}, "Fail to process gap")
        self.assertEqual(sequence_cache._objects[seq], message4, "Fail to process gap")
        self.assertEqual(sequence_cache._time_indexes[timestamp['epochSecond']], [seq, seq], "Fail to process gap")

    def test_ob_add_order(self):
        order_book = {"ask": {}, "bid": {}, "status": "?", "aggr_max_levels": 15}
        # Add new order
        ord_id = "id1"
        price = 1.5
        size = 10
        side = "bid"
        err, ob = ob_add_order(ord_id, price, size, side, "1", order_book)
        self.assertEqual(err, {}, "Failed to add new order")
        self.assertEqual(order_book[side][price][ord_id], size, "Failed to add new order")
        self.assertEqual(ob[0], order_book, "Failed to add new order")
        # Add again
        err, ob = ob_add_order(ord_id, price, size, side, "2", order_book)
        self.assertEqual(err, {"error": "Order already exists in book"}, "Unable to process dup error")
        self.assertEqual(ob, [], "Unable to process dup error")
        # Add new order with same price
        ord_id = "id2"
        price = 1.5
        size = 100
        side = "bid"
        err, ob = ob_add_order(ord_id, price, size, side, "1", order_book)
        self.assertEqual(err, {}, "Failed to add new order")
        self.assertEqual(len(order_book[side][price]), 2, "Failed to add new order")
        self.assertEqual(order_book[side][price][ord_id], size, "Failed to add new order")
        self.assertEqual(ob[0], order_book, "Failed to add new order")
        # Add new order with diff price
        ord_id = "id3"
        price = 1.6
        size = 100
        side = "bid"
        err, ob = ob_add_order(ord_id, price, size, side, "2", order_book)
        self.assertEqual(err, {}, "Failed to add new order")
        self.assertEqual(len(order_book[side]), 2, "Failed to add new order")
        self.assertEqual(order_book[side][price][ord_id], size, "Failed to add new order")
        self.assertEqual(ob[0], order_book, "Failed to add new order")
