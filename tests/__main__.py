"""
Runs all tests
"""

import unittest
from unittest import defaultTestLoader

from .handler import RequestHandlerTestCase
from .msgpack_ext import MsgPackTestCase


suite = unittest.TestSuite()

testcases = [RequestHandlerTestCase, MsgPackTestCase]

for testcase in testcases:
    suite.addTests(defaultTestLoader.loadTestsFromTestCase(testcase))

unittest.TextTestRunner().run(suite)
