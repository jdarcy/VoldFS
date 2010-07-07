"""
Copyright (c) 2010, Jeff Darcy <jeff@pl.atyp.us>

Permission to use, copy, modify, and/or distribute this software for any
purpose with or without fee is hereby granted, provided that the above
copyright notice and this permission notice appear in all copies.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
"""

import stat
import struct
import sys
import time

import jlog
log = jlog.logger(jlog.NORMAL)

# mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime, tree_depth
INODE_FMT = "!IQQIIIQIIII"
INODE_SZ = struct.calcsize(INODE_FMT)

# A pointer is a 16-bit node, 16-bit boot generation, and 32-bit sequence number
PTR_FMT = "!HHI"
PTR_SZ = struct.calcsize(PTR_FMT)

BLOCK_SZ = 1024	# for debugging
assert (BLOCK_SZ % PTR_SZ) == 0
PTRS_PER_BLOCK = BLOCK_SZ / PTR_SZ

# In a pointer, indicates that the pointer is Nil.
INVALID_NODE = 0

# Values for generating new keys.
NODE_ID = 1
boot_gen = 0
sequence = 0

def get_new_key ():
	global sequence
	sequence += 1
	return struct.pack(PTR_FMT,NODE_ID,boot_gen,sequence)

class BlockSet:
	def __init__ (self, getter):
		self.get_func = getter
		self.old_blocks = []
		self.new_blocks = {}
		self.free_list = []
	def get (self, key):
		# We very much do *not* want to store an existing key into
		# new_blocks here, because that would lead to write-in-place
		# and we want COW.  We could allocate a new key here, but
		# instead we defer that until we're sure we have new data
		# to go with it.
		try:
			return self.new_blocks[key]
		except KeyError:
			return self.get_func(key)[0]
	def put (self, key, value):
		if key not in self.new_blocks:
			key = self.alloc()
		self.new_blocks[key] = value
		return key
	def alloc (self):
		try:
			return self.free_list.pop()
		except IndexError:
			return get_new_key()
	def reset (self):
		self.old_blocks = []
		self.free_list += self.new_blocks.keys()
		self.new_blocks = {}
	def flush (self, putter):
		for k, v in self.new_blocks.items():
			putter(k,v)
		# TBD: GC anything in old_blocks/free_list

class IoOp:
	def __init__ (self, op, key):
		self.key = key
		self.name = "%s,%s" % (op, repr(key))
		self.version = None
	def __repr__ (self):
		if self.version:
			return "IoOp(%s,%d)" % (self.name,
				self.version.entries[0].version)
		else:
			return "IoOp(%s)" % self.name
	def set_version (self, vec):
		self.version = vec

class FS:
	def __init__ (self, store):
		self.store = store
	def get_value (self, key):
		# This might throw a VoldemortException.
		versions = self.store.get(key)
		if len(versions) != 1:
			for data, clock in versions:
				for e in clock:
					print e
			errstr = "bad version count %d" % len(versions)
			raise RuntimeError, errstr
		first = versions[0]
		if len(first) != 2:
			raise RuntimeError, "malformed data/version tuple"
		return first
	def get_inode (self, key):
		data, vector = self.get_value(key)
		if len(data) < INODE_SZ:
			raise RuntimeError, "bad inode size"
		return data, vector
	def get_block (self, key):
		node, boot, seq = struct.unpack(PTR_FMT,key)
		if node == INVALID_NODE:
			return False, None
		data, vector = self.get_value(key)
		if len(data) != BLOCK_SZ:
			raise RuntimeError, "bad block size %u" % len(data)
		return data, vector
	def put_value (self, key, data, version=None):
		if version != None:
			version.entries[0].version += 1
		return self.store.put(key,data,version)
	def create_inode (self, key, mode, size=0, depth=0, entries=[]):
		mode &= 0777
		idata = struct.pack(INODE_FMT,stat.S_IFREG|mode,0,0,0,0,0,
			size,0,0,0,depth)
		idata += struct.pack('%ds'%BLOCK_SZ,'')
		for index, dst in entries:
			pdata = struct.pack(PTR_FMT,NODE_ID,0,dst)
			offset = INODE_SZ + index * PTR_SZ
			idata = idata[:offset] + pdata + idata[offset+PTR_SZ:]
		return self.put_value(key,idata)
	def get_data (self, key, offset, length, io=None):
		if not io:
			io = IoOp("get",key)
		idata, vector = self.get_inode(key)
		io.set_version(vector)
		inode = struct.unpack(INODE_FMT,idata[:INODE_SZ])
		size = inode[6]
		if offset >= size:
			log.it(jlog.DEBUG,"beyond EOF")
			return ''
		left = size - offset
		if length > left:
			length = left
		left = BLOCK_SZ - (offset % BLOCK_SZ)
		if length > left:
			length = left
		depth = inode[10]
		if not depth:
			log.it(jlog.DEBUG,"embedded: %d at %d" % (
				length, offset))
			# TBD: atime
			return idata[INODE_SZ+offset:INODE_SZ+offset+length]
		bnum = offset / BLOCK_SZ
		offset %= BLOCK_SZ
		path = []
		while depth:
			path.insert(0,bnum%PTRS_PER_BLOCK)
			bnum /= PTRS_PER_BLOCK
			depth -= 1
		log.it(jlog.DEBUG,"indirect: path %s" % repr(path))
		ptr_off = INODE_SZ + path[0] * PTR_SZ
		new_key = idata[ptr_off:ptr_off+PTR_SZ]
		try:
			data, vector = self.get_value(new_key)
		except:	# TBD: catch specific missing-data exception(s)
			# Fell into a hole.
			return struct.pack('%ds'%length,'')
		for index in path[1:]:
			ptr_off = index * PTR_SZ
			new_key = data[ptr_off:ptr_off+PTR_SZ]
			try:
				data, vector = self.get_value(new_key)
			except:	# TBD: catch specific missing-data exception(s)
				# Fell into a hole.
				return struct.pack('%ds'%length,'')
		return data[offset:offset+length]
	def ensure_size (self, key, new_size):
		while True:
			idata, vector = self.get_inode(key)
			inode = struct.unpack(INODE_FMT,idata[:INODE_SZ])
			old_size = inode[6]
			if new_size <= old_size:
				return idata, vector
			old_depth = inode[10]
			new_depth = 0
			blocks = (new_size + BLOCK_SZ - 1) / BLOCK_SZ
			while blocks > 1:
				new_depth += 1
				blocks += (PTRS_PER_BLOCK - 1)
				blocks /= PTRS_PER_BLOCK
			if new_depth <= old_depth:
				return idata, vector
			log.it(jlog.DEBUG, "expanding from %d" % old_depth)
			new_key = get_new_key()
			self.put_value(new_key,idata[INODE_SZ:])
			old_inode = struct.unpack(INODE_FMT,idata[:INODE_SZ])
			new_inode = old_inode[:10] + (old_inode[10]+1,)
			new_idata = apply(struct.pack,(INODE_FMT,)+new_inode)
			new_idata += new_key
			new_idata += struct.pack('%ds'%(BLOCK_SZ-PTR_SZ),'')
			try:
				self.put_value(key,new_idata,vector)
				old_depth += 1
				if old_depth >= new_depth:
					log.it(jlog.DEBUG,
						"new depth = %d" % new_depth)
					return new_idata, vector
			except:	# TBD: catch conflict-specific error(s)
				pass	# TBD: delete new_key
	def link_one (self, key, path, dkey, bset):
		if len(path) == 0:
			return dkey
		my_index = path.pop()
		node, boot, seq = struct.unpack(PTR_FMT,key)
		if node == INVALID_NODE:
			data = struct.pack('%ds'%BLOCK_SZ,'')
		else:
			data = bset.get(key)
		offset = my_index * PTR_SZ
		ckey = data[offset:offset+PTR_SZ]
		ckey = self.link_one(ckey,path,dkey,bset)
		try:
			data = data[:offset] + ckey + data[offset+PTR_SZ:]
		except:
			import pdb
			pdb.set_trace()
		return bset.put(key,data)
	def put_block (self, idata, bnum, dkey, bset):
		inode = struct.unpack(INODE_FMT,idata[:INODE_SZ])
		path = []
		for i in range(inode[10]):
			# NB: order is from least significant to most
			path.append(bnum%PTRS_PER_BLOCK)
			bnum /= PTRS_PER_BLOCK
		assert len(path) > 0
		my_index = path.pop()
		offset = INODE_SZ + my_index * PTR_SZ
		ckey = idata[offset:offset+PTR_SZ]
		ckey = self.link_one(ckey,path,dkey,bset)
		idata = idata[:offset] + ckey + idata[offset+PTR_SZ:]
		return idata
	def put_once (self, io, idata, data, chunks, bset):
		# Make sure every block is in store, not necessarily linked.
		index = 0
		for mem_off, dsk_off, length, key in chunks:
			if length == BLOCK_SZ:
				if key:
					continue
				new_data = data[mem_off:(mem_off+length)]
			else:
				tmp_off = (dsk_off / BLOCK_SZ) * BLOCK_SZ
				old_data = self.get_data(io.key,tmp_off,
					BLOCK_SZ, io)
				short = BLOCK_SZ - len(old_data)
				if short:
					extra = struct.pack('%ds'%short,'')
					old_data += extra
				tmp_off = dsk_off % BLOCK_SZ
				new_data = old_data[:tmp_off] +\
					   data[mem_off:(mem_off+length)] +\
					   old_data[tmp_off+length:]
			if not key:
				key = get_new_key()
			self.put_value(key,new_data)
			chunks[index][3] = key
			index += 1
		# Link each block to the inode.
		for mem_off, dsk_off, length, key in chunks:
			idata = self.put_block(idata,dsk_off/BLOCK_SZ,key,bset)
		return idata
	def fix_size (self, idata, new_size):
		inode = list(struct.unpack(INODE_FMT,idata[:INODE_SZ]))
		inode[6] = new_size
		idata = apply(struct.pack, (INODE_FMT,) + tuple(inode)) + \
			idata[INODE_SZ:]
		return idata
	def put_data (self, key, offset, data):
		io = IoOp("put",key)
		new_size = offset + len(data)
		idata, vector = self.ensure_size(key,new_size)
		io.set_version(vector)
		inode = struct.unpack(INODE_FMT,idata[:INODE_SZ])
		old_size = inode[6]
		depth = inode[10]
		# Try the easy path if we can.
		while (depth == 0) and (new_size <= BLOCK_SZ):
			log.it(jlog.DEBUG,"taking short path")
			b_offset = INODE_SZ + offset
			e_offset = INODE_SZ + new_size
			try:
				if new_size > old_size:
					idata = self.fix_size(idata, new_size)
				idata = idata[:b_offset] + \
					data + idata[e_offset:]
				self.put_value(key,idata,vector)
				return len(data)
			except:	# TBD: catch conflict-specific error(s)
				idata, vector = self.get_inode(key)
		# We don't want anyone manipulating inode below here.
		del inode
		# Make a list of block-level operations.
		chunks = []
		mem_offset = 0
		dsk_offset = offset
		total_len = len(data)
		while total_len > 0:
			in_block = BLOCK_SZ - (dsk_offset % BLOCK_SZ)
			if total_len <= in_block:
				this_len = total_len
			else:
				this_len = in_block
			chunks.append([mem_offset,dsk_offset,this_len,None])
			mem_offset += this_len
			dsk_offset += this_len
			total_len -= this_len
		bset = BlockSet(self.get_value)
		# Try to apply the list until we succeed.
		while True:
			try:
				idata = self.put_once(io,idata,data,
							chunks,bset)
				bset.flush(self.put_value)
				if new_size > old_size:
					idata = self.fix_size(idata,new_size)
				print "sleeping..."
				time.sleep(10)
				print "awake"
				self.put_value(key,idata,io.version)
				break
			except:	# TBD: catch conflict-specific error(s)
				bset.reset()
				idata, vector = self.get_inode(key)
				new_old_size = struct.unpack(INODE_FMT,
					idata[:INODE_SZ])[6]
				if new_old_size != old_size:
					old_size = new_old_size
		return len(data)
	def dump_pointers (self, data, offset, cur_depth, max_depth):
		if cur_depth >= max_depth:
			return
		i = 0
		while i < PTRS_PER_BLOCK:
			raw = data[offset:(offset+PTR_SZ)]
			node, boot, seq = struct.unpack(PTR_FMT,raw)
			if node != INVALID_NODE:
				log.it(jlog.DEBUG,"%*s %u -> %u:%u:%u" % (
					cur_depth*2,"",i, node, boot, seq))
				data2, vector = self.get_block(raw)
				self.dump_pointers(data2,0,cur_depth+1,
					max_depth)
			offset += PTR_SZ
			i += 1

	def dump (self, key):
		idata, vector = self.get_inode(key)
		inode = struct.unpack(INODE_FMT,idata[:INODE_SZ])
		log.it(jlog.DEBUG,"mode %o, size %u, depth %u" % (
			inode[0], inode[6], inode[10]))
		self.dump_pointers(idata,INODE_SZ,0,inode[10])

