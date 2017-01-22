"""
Runs all tests
"""

import unittest

from . import full_suite

unittest.TextTestRunner().run(full_suite())
