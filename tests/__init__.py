import unittest


def get_tests():
    return full_suite()


def full_suite():
    from .handler import RequestHandlerTestCase
    from .msgpack_ext import MsgPackTestCase

    handler_suite = unittest.TestLoader().loadTestsFromTestCase(RequestHandlerTestCase)
    msgpack_suite = unittest.TestLoader().loadTestsFromTestCase(MsgPackTestCase)

    return unittest.TestSuite([handler_suite, msgpack_suite])
