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

import string
import traceback

import memcache
if "cas" not in dir(memcache.Client):
	raise RuntimeError, "wrong python-memcache version"

class FakeVersion:
	def __init__ (self, n):
		self.version = n

class FakeVector:
	def __init__ (self, n):
		self.entries = [FakeVersion(n)]

# Memcached keys are not binary-safe, so we convert to a strict text form.
def encode (istr):
	ostr = ""
	for c in istr:
		ostr += '%02x' % ord(c)
	return ostr

def decode (istr):
	ostr = ""
	ilen = len(istr)
	i = 0
	while i < ilen:
		ostr += chr(int(istr[i:i+2],16))
		i += 2
	return ostr

# The memcached client stores CAS versions itself instead of passing them
# back to us (like Voldemort does).  Yes, "CAS" is a misnomer for what's
# really a conditional write.  Anyway, what's worse is that they neither
# prune the stored-version list nor update its contents after a successful
# operation.  The first means that the list tends to grow without bound.
# The second means that the list can contain stale information, so e.g. if
# we put once from within expand_inode and try to put again to update an
# indirect block, the second put will fail.
#
# We could deal with the stale-version problem by updating the version list
# (self.mc.cas_ids) in place, but then we'd still have the unchecked-growth
# problem.  What we do instead is take the information out of cas_ids and
# repopulating it only during the ensuing CAS operation.  In other words, we
# essentially convert the stored-internally memcached programming model into
# the passed-back Voldemort model.

class StoreClient:
	def __init__ (self, store_name, bootstrap_urls):
		s_list = []
		for host, port in bootstrap_urls:
			# TBD: deal with port defaults properly
			s_list.append("%s:%d"%(host,11211))
		self.mc = memcache.Client(s_list)
		self.auto_mkfs = False
		self.log_ops = True
		self.ops = []
	def get (self, key):
		k2 = encode(key)
		data = self.mc.gets(k2)
		if not data:
			print "get(%s) FAILED" % k2
			raise RuntimeError, "bad get"
		self.log(("get",k2,len(data)))
		version = self.mc.cas_ids[k2]
		del self.mc.cas_ids[k2]
		return [(data,FakeVector(version))]
	def put (self, key, data, version):
		k2 = encode(key)
		if version:
			self.mc.cas_ids[k2] = version.entries[0].version - 1
			result = self.mc.cas(k2,data)
			del self.mc.cas_ids[k2]
		else:
			result = self.mc.set(k2,data)
		self.log(("cas",k2,result))
		if result or not version:
			return result
		print "cas(%s,%d) FAILED" % (k2, len(data))
		# TBD: figure out why
		self.dump_log()
		self.mc.set(k2,data)
		result = self.mc.set(k2,data)
		self.log(("set",k2,result))
		if not result:
			print "set(%s,%d) FAILED" % (k2, len(data))
		return result
	def log (self, info):
		if not self.log_ops:
			return
		if len(self.ops) >= 10:
			del self.ops[0]
		self.ops.append((info,traceback.format_stack()))

	def dump_log (self):
		for info, tb in self.ops:
			op, key, result = info
			print "%s(%s) => %s" % (op, key, repr(result))
			print string.join(tb)
