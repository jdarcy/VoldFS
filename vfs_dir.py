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

import hashlib
import stat
import struct
import sys
from vfs_base import *

import jlog
log = jlog.logger(jlog.NORMAL)

BUCKET_HDR_FMT = "!c3x"
BUCKET_HDR_SZ = struct.calcsize(BUCKET_HDR_FMT)
MAX_NAME_LEN = 55
ENTRY_FMT = '!%dp%ds' % (MAX_NAME_LEN+1, PTR_SZ)
ENTRY_SZ = struct.calcsize(ENTRY_FMT)
ENTRIES_PER_BUCKET = 4
BUCKET_FMT = BUCKET_HDR_FMT + ('%dx' % (ENTRY_SZ * ENTRIES_PER_BUCKET))
BUCKET_SZ = struct.calcsize(BUCKET_FMT)
BUCKET_DSZ = BUCKET_SZ - BUCKET_HDR_SZ
log.it(jlog.DEBUG,
	"bucket size = %d header + %d data" % (BUCKET_HDR_SZ, BUCKET_DSZ))

# Get our total size between 0.75x and 1.5x BLOCK_SZ.
BUCKET_AREA = (BLOCK_SZ - INODE_SZ) * 3 / 4
BUCKET_SHIFT = 0
while (BUCKET_SZ << BUCKET_SHIFT) <= BUCKET_AREA:
	BUCKET_SHIFT += 1
BUCKETS_PER_BLOCK = 1 << BUCKET_SHIFT
log.it(jlog.DEBUG,
	"bucket shift/count = %d/%d" % (BUCKET_SHIFT, BUCKETS_PER_BLOCK))

PTR_SHIFT = 0
while (PTR_SZ << (PTR_SHIFT + 1)) <= BUCKET_DSZ:
	PTR_SHIFT += 1
PTRS_PER_BUCKET = 1 << PTR_SHIFT
log.it(jlog.DEBUG,"pointer shift/count = %d/%d" % (PTR_SHIFT, PTRS_PER_BUCKET))

DIR_BLK_SZ = BUCKET_SZ * BUCKETS_PER_BLOCK
log.it(jlog.DEBUG,"directory block size = %d (plus %d for inode)" % (
	DIR_BLK_SZ, INODE_SZ))

PROTO_DBUCKET = struct.pack(BUCKET_FMT,'D')
PROTO_IBUCKET = struct.pack(BUCKET_FMT,'I')

class DupFileExc (Exception):
	def __init__ (self, name):
		self.name = name

class BadStateExc (Exception):
	pass

class DirOp:
	def __init__ (self, fs, key):
		self.fs = fs
		self.key = key
		self.cache = {}

	def create (self, mode):
		mode &= 0777
		idata = struct.pack(INODE_FMT,stat.S_IFDIR|mode,
			0,0,0,0,0,0,0,0,0,0)
		return self.fs.put_value(self.key,
			idata + PROTO_DBUCKET * BUCKETS_PER_BLOCK)

	def split (self, bdata, used):
		log.it(jlog.DEBUG,"*** BEGIN SPLIT")
		new_bdata = PROTO_IBUCKET
		e_off = BUCKET_HDR_SZ
		for i in range(ENTRIES_PER_BUCKET):
			name, ptr = struct.unpack(ENTRY_FMT,
				bdata[e_off:e_off+ENTRY_SZ])
			e_off += ENTRY_SZ
			log.it(jlog.DEBUG,"pushing %s down" % name)
			hashobj = hashlib.md5()
			hashobj.update(name)
			hash = struct.unpack('QQ',hashobj.digest())[0]
			new_bdata = self.add_indirect(new_bdata,hash,used,
				name,ptr)
		log.it(jlog.DEBUG,"*** END SPLIT")
		return new_bdata

	def add_direct (self, bdata, hash, used, name, ptr):
		log.it(jlog.DEBUG,"in add_direct(0x%x/%d,%s)" % (
			hash, used, name))
		is_del = (ptr == None)
		found = 0
		e_off = BUCKET_HDR_SZ
		for i in range(ENTRIES_PER_BUCKET):
			name2, ptr2 = struct.unpack(ENTRY_FMT,
				bdata[e_off:e_off+ENTRY_SZ])
			log.it(jlog.DEBUG,"comparing %s to %s" % (name2, name))
			if is_del:
				found = i + 1
				break
			else:
				if name2 == name:
					raise DupFileExc(name)
				if (not found) and (name2 == ''):
					found = i + 1
			e_off += ENTRY_SZ
		if not found:
			if is_del:
				raise DupFileExc(name)
			bdata = self.split(bdata,used)
			return self.add_indirect(bdata,hash,used,name,ptr)
		i = found - 1
		log.it(jlog.DEBUG,"using entry %d" % i)
		e_off = BUCKET_HDR_SZ + ENTRY_SZ * i
		if is_del:
			print "deleting %s" % name
			edata = struct.pack(ENTRY_FMT,"","")
		else:
			edata = struct.pack(ENTRY_FMT,name,ptr)
		return bdata[:e_off] + edata + bdata[e_off+ENTRY_SZ:]

	def add_indirect (self, bdata, hash, used, name, ptr):
		log.it(jlog.DEBUG,"in add_indirect(0x%x/%d,%s)" % (
			hash, used, name))
		index = (hash >> used) % PTRS_PER_BUCKET
		used += PTR_SHIFT
		log.it(jlog.DEBUG,"going into sub-block %d" % index)
		p_off = BUCKET_HDR_SZ + PTR_SZ * index
		old_key = bdata[p_off:p_off+PTR_SZ]
		node, boot, seq = struct.unpack(PTR_FMT,old_key)
		if node == INVALID_NODE:
			log.it(jlog.DEBUG,"  creating new sub-block")
			nk_data = PROTO_DBUCKET * BUCKETS_PER_BLOCK
		else:
			log.it(jlog.DEBUG,
				"  getting old sub-block %s" % repr(old_key))
			nk_data = self.bset.get(old_key)
		nk_data = self.add_once(nk_data,0,hash,used,name,ptr)
		new_key = self.bset.put(old_key,nk_data)
		log.it(jlog.DEBUG,"new_key = %s" % repr(new_key))
		return bdata[:p_off] + new_key + bdata[p_off+PTR_SZ:]

	def add_once (self, idata, offset, hash, used, name, ptr):
		log.it(jlog.DEBUG,"in add_once(%d,0x%x/%d,%s)" % (
			offset, hash, used, name))
		index = (hash >> used) % BUCKETS_PER_BLOCK
		log.it(jlog.DEBUG,"using bucket %d" % index)
		used += BUCKET_SHIFT
		offset += BUCKET_SZ * index
		bdata = idata[offset:offset+BUCKET_SZ]
		state = struct.unpack(BUCKET_HDR_FMT,bdata[:BUCKET_HDR_SZ])[0]
		if state == 'D':
			bdata = self.add_direct(bdata,hash,used,name,ptr)
		elif state == 'I':
			bdata = self.add_indirect(bdata,hash,used,name,ptr)
		else:
			raise BadStateExc
		return idata[:offset] + bdata + idata[offset+BUCKET_SZ:]

	def add (self, name, ptr):
		if len(name) > MAX_NAME_LEN:
			raise KeyError, "name too long"
		hashobj = hashlib.md5()
		hashobj.update(name)
		hash = struct.unpack("QQ",hashobj.digest())[0]
		log.it(jlog.DEBUG,"%s hashes to 0x%x" % (name, hash))
		self.bset = BlockSet(self.fs.get_value)
		while True:
			try:
				idata, vector = self.fs.get_value(self.key)
				idata = self.add_once(idata,INODE_SZ,
					hash, 0, name, ptr)
				self.bset.flush(self.fs.put_value)
				self.fs.put_value(self.key,idata,vector)
				break
			except DupFileExc:
				etype, dfe, stack = sys.exc_info()
				log.it(jlog.DEBUG,
					"duplicate detected for %s" % dfe.name)
				return
			"""
			except: # TBD: catch conflict-specific exception(s)
				self.bset.reset()
			"""

	def lookup_one (self, name, data, offset, hash, used):
		log.it(jlog.DEBUG,"in lookup_one(%s,%d,0x%x/%d)" % (
			name, offset, hash, used))
		index = (hash >> used) % BUCKETS_PER_BLOCK
		used += BUCKET_SHIFT
		log.it(jlog.DEBUG,"  using bucket %d" % index)
		b_off = offset + BUCKET_SZ * index
		state = struct.unpack(BUCKET_HDR_FMT,
			data[b_off:b_off+BUCKET_HDR_SZ])[0]
		if state == 'D':
			e_off = b_off + BUCKET_HDR_SZ
			for i in range(ENTRIES_PER_BUCKET):
				name2, ptr = struct.unpack(ENTRY_FMT,
					data[e_off:e_off+ENTRY_SZ])
				e_off += ENTRY_SZ
				log.it(jlog.DEBUG,"  direct compare %s" % name2)
				if name2 == name:
					log.it(jlog.DEBUG,
						"  got match (%s)" % repr(ptr))
					return ptr
			log.it(jlog.DEBUG,"  no match")
			return None
		if state == 'I':
			index = (hash >> used) % PTRS_PER_BUCKET
			used += PTR_SHIFT
			log.it(jlog.DEBUG,"  going to sub-block %d" % index)
			p_off = b_off + BUCKET_HDR_SZ + PTR_SZ * index
			key = data[p_off:p_off+PTR_SZ]
			node, boot, seq = struct.unpack(PTR_FMT,key)
			if node == INVALID_NODE:
				log.it(jlog.DEBUG,"  no such sub-block")
				return None
			data, vector = self.fs.get_value(key)
			return self.lookup_one(name,data,0,hash,used)
		raise BadStateExc

	def lookup (self, name):
		if len(name) > MAX_NAME_LEN:
			raise KeyError, "name too long"
		hashobj = hashlib.md5()
		hashobj.update(name)
		hash = struct.unpack("QQ",hashobj.digest())[0]
		log.it(jlog.DEBUG,"%s hashes to 0x%x" % (name, hash))
		data, vector = self.fs.get_value(self.key)
		return self.lookup_one(name,data,INODE_SZ,hash,0)

	# Unlike most other situations, we do want to keep using cached
	# values so that a series of enum calls use a consistent view even
	# if other modifications occur in the meantime.
	def get_cached (self, key):
		try:
			data = self.cache[key]
		except KeyError:
			data, vector = self.fs.get_value(key)
			self.cache[key] = data
		return data

	def enum_direct (self, bdata, xhash, used, first):
		log.it(jlog.DEBUG,"enum_direct(0x%x/%d)" % (xhash, used))
		if first:
			index = (self.entry >> used) % ENTRIES_PER_BUCKET
		else:
			index = 0
		mask = (1 << used) - 1
		offset = BUCKET_HDR_SZ + ENTRY_SZ * index
		for e_idx in range(index,ENTRIES_PER_BUCKET):
			log.it(jlog.DEBUG," e_idx = %d" % e_idx)
			edata = bdata[offset:offset+ENTRY_SZ]
			offset += ENTRY_SZ
			mask = (1 << used) - 1
			yhash = ((xhash & mask) | (e_idx << used)) + 2
			log.it(jlog.DEBUG,'self.entry = 0x%x, yhash = 0x%x' % (
				self.entry, yhash))
			if yhash == (self.entry + 2):
				if self.orig_entry >= 2:
					log.it(jlog.DEBUG,"found last entry")
					continue
			name, ptr = struct.unpack(ENTRY_FMT,edata)
			if name == "":
				continue
			if self.callback(name,ptr,yhash):
				return True
			self.cb_calls += 1
		return False

	def enum_indirect (self, bdata, xhash, used, first):
		log.it(jlog.DEBUG,"enum_indirect(0x%x/%d)" % (xhash, used))
		if first:
			index = (self.entry >> used) % PTRS_PER_BUCKET
		else:
			index = 0
		mask = (1 << used) - 1
		used += PTR_SHIFT
		offset = BUCKET_HDR_SZ + PTR_SZ * index
		for p_idx in range(index,PTRS_PER_BUCKET):
			log.it(jlog.DEBUG," p_idx = %d" % p_idx)
			key = bdata[offset:offset+PTR_SZ]
			offset += PTR_SZ
			node, boot, seq = struct.unpack(PTR_FMT,key)
			if node == INVALID_NODE:
				continue
			data = self.get_cached(key)
			yhash = (xhash & mask) | (p_idx << (used-PTR_SHIFT))
			if self.enum_one(data,0,yhash,used,first):
				return True
			first = False
		return False

	def enum_one (self, data, offset, xhash, used, first):
		log.it(jlog.DEBUG,"enum_one(0x%x/%d)" % (xhash, used))
		if first:
			index = (self.entry >> used) % BUCKETS_PER_BLOCK
		else:
			index = 0
		mask = (1 << used) - 1
		used += BUCKET_SHIFT
		for b_idx in range(index,BUCKETS_PER_BLOCK):
			log.it(jlog.DEBUG," b_idx = %d" % b_idx)
			b_off = offset + BUCKET_SZ * b_idx
			bdata = data[b_off:b_off+BUCKET_SZ]
			hdr = struct.unpack(BUCKET_HDR_FMT,
				bdata[:BUCKET_HDR_SZ])
			yhash = (xhash & mask) | (b_idx << (used-BUCKET_SHIFT))
			if hdr[0] == 'D':
				if self.enum_direct(bdata,yhash,used,first):
					return True
			elif hdr[0] == 'I':
				if self.enum_indirect(bdata,yhash,used,first):
					return True
			else:
				raise BadStateExc
			first = False
		return False

	def enum (self, callback, entry=0):
		log.it(jlog.DEBUG,"enum(0x%x)" % entry)
		data = self.get_cached(self.key)
		# TBD: check that it's a directory
		# TBD: add real entries for . and .. during mkdir
		self.orig_entry = entry
		if entry == 0:
			if callback(".","",1):
				return
			entry += 1
		if entry == 1:
			if callback("..","",2):
				return
			entry += 1
		self.entry = entry - 2
		self.callback = callback
		self.cb_calls = 0
		self.enum_one(data,INODE_SZ,0,0,True)
		if self.cb_calls:
			return False	# not done
		self.cache = {}
		return True

	def dump_direct (self, indent, index, bdata):
		log.it(jlog.DEBUG,"%*sdirect bucket %d" % (indent, '', index))
		offset = BUCKET_HDR_SZ
		for i in range(ENTRIES_PER_BUCKET):
			edata = bdata[offset:offset+ENTRY_SZ]
			offset += ENTRY_SZ
			name, ptr = struct.unpack(ENTRY_FMT,edata)
			if name != '':
				log.it(jlog.DEBUG,"%*sentry %d => %s" % (
					indent+1,'',i,name))

	def dump_indirect (self, indent, index, bdata):
		log.it(jlog.DEBUG,"%*sindirect bucket %d" % (indent,'',index))
		offset = BUCKET_HDR_SZ
		for i in range(PTRS_PER_BUCKET):
			pdata = bdata[offset:offset+PTR_SZ]
			offset += PTR_SZ
			node, boot, seq = struct.unpack(PTR_FMT,pdata)
			if node != INVALID_NODE:
				log.it(jlog.DEBUG,"%*ssub-block %d -> %s" % (
					indent+1,'',i, repr(pdata)))
				self.dump(pdata,indent+2,0)

	def dump (self, key, indent=0, offset=INODE_SZ):
		idata, vector = self.fs.get_value(key)
		for i in range(BUCKETS_PER_BLOCK):
			bdata = idata[offset:offset+BUCKET_SZ]
			offset += BUCKET_SZ
			hdr = bdata[:BUCKET_HDR_SZ]
			state = struct.unpack(BUCKET_HDR_FMT,hdr)[0]
			if state == 'D':
				self.dump_direct(indent,i,bdata)
			elif state == 'I':
				self.dump_indirect(indent,i,bdata)
			else:
				raise BadStateExc
				
def lookup (fs, root, path):
	ptr = root
	for part in path.split("/"):
		if part == "":
			continue
		d = DirOp(fs,ptr)
		ptr = d.lookup(part)
		if ptr == None:
			break
	return ptr

def mkdir (fs, key, mode):
	d = DirOp(fs,key)
	return d.create(mode)

def link (fs, parent, name, child):
	d = DirOp(fs,parent)
	return d.add(name,child)

def unlink (fs, parent, name):
	d = DirOp(fs,parent)
	return d.add(name,None)

def enum (fs, key, callback, offset=0):
	d = DirOp(fs,key)
	return d.enum(callback,offset)
