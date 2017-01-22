import unittest
from unittest import defaultTestLoader

from .handler import RequestHandlerTestCase
from .msgpack_ext import MsgPackTestCase


def get_tests():
    return full_suite()


def full_suite():

    suite = unittest.TestSuite()

    testcases = [RequestHandlerTestCase, MsgPackTestCase]

    for testcase in testcases:
        suite.addTests(defaultTestLoader.loadTestsFromTestCase(testcase))

    return suite
