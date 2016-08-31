"""
Extends json encoder such that it can handle Python slice objects
"""

import json


class SliceEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, slice):
            # note that start, stop, and step can again be slice objects
            return {
                '__slice__': True,
                'start': obj.start,
                'stop': obj.stop,
                'step': obj.step,
            }

        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)
