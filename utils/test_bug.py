import logging

import hurraypy as hr


logger = logging.getLogger('hurraypy')
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
console.setFormatter(logging.Formatter('%(levelname)s --- %(message)s'))
logger.addHandler(console)
logger.setLevel(logging.DEBUG)

conn = hr.connect('localhost', '2222')

f2 = conn.create_file("test_asdf.h5", overwrite=True)
f2.delete()
# f2 = conn.create_file("test_asdf.h5", overwrite=True)

conn.close()
