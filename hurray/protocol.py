# Copyright (c) 2016, Meteotest
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#    * Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#    * Redistributions in binary form must reproduce the above copyright
#      notice, this list of conditions and the following disclaimer in the
#      documentation and/or other materials provided with the distribution.
#    * Neither the name of Meteotest nor the
#      names of its contributors may be used to endorse or promote products
#      derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

# note that the hurray server and the client contain the same file

MSG_LEN = 4
PROTOCOL_VER = 1

# command keywords
CMD_KW_CMD = 'cmd'
CMD_KW_ARGS = 'args'
CMD_KW_DATA = 'data'
CMD_KW_PATH = 'path'

# note that these are kwargs that are passed 1:1 to h5py (don't rename!)
CMD_KW_SHAPE = 'shape'
CMD_KW_DTYPE = 'dtype'
CMD_KW_COMPRESSION = 'compression'
CMD_KW_COMPRESSION_OPTS = 'compression_opts'
CMD_KW_CHUNKS = 'chunks'
CMD_KW_FILLVALUE = 'fillvalue'
CMD_KW_REQUIRE_EXACT = 'exact'

CMD_KW_KEY = 'key'
CMD_KW_DB = 'db'
CMD_KW_DB_RENAMETO = 'db_new_name'
CMD_KW_OVERWRITE = 'overwrite'
CMD_KW_STATUS = 'status'

# commands
CMD_CREATE_DATABASE = 'create_db'
CMD_RENAME_DATABASE = 'rename_db'
CMD_DELETE_DATABASE = 'delete_db'
CMD_USE_DATABASE = 'use_db'
CMD_LIST_DATABASES = 'list_dbs'
CMD_CREATE_GROUP = 'create_group'
CMD_REQUIRE_GROUP = 'require_group'
CMD_CREATE_DATASET = 'create_dataset'
CMD_REQUIRE_DATASET = 'require_dataset'
CMD_GET_NODE = 'get_node'
CMD_CONTAINS = 'contains'
CMD_GET_KEYS = 'get_keys'
CMD_GET_TREE = 'get_tree'
CMD_GET_FILESIZE = 'get_filesize'
CMD_SLICE_DATASET = 'slice_dataset'
CMD_BROADCAST_DATASET = 'broadcast_dataset'

# attribute commands
CMD_ATTRIBUTES_GET = 'attrs_getitem'
CMD_ATTRIBUTES_SET = 'attrs_setitem'
CMD_ATTRIBUTES_CONTAINS = 'attrs_contains'
CMD_ATTRIBUTES_KEYS = 'attrs_keys'

# response keywords etc.
RESPONSE_H5FILE = 'h5file'
RESPONSE_NODE_TYPE = 'nodetype'
RESPONSE_NODE_SHAPE = 'shape'
RESPONSE_NODE_DTYPE = 'dtype'
RESPONSE_NODE_PATH = 'nodepath'
RESPONSE_NODE_KEYS = 'nodekeys'
RESPONSE_NODE_TREE = 'nodetree'
RESPONSE_ATTRS_CONTAINS = 'contains'
RESPONSE_ATTRS_KEYS = 'keys'
RESPONSE_DATA = 'data'

NODE_TYPE_FILE = 'file'
NODE_TYPE_GROUP = 'group'
NODE_TYPE_DATASET = 'dataset'
