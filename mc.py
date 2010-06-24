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

import memcache
if "cas" not in dir(memcache.Client):
	raise RuntimeError, "wrong python-memcache version"

class FakeVersion:
	def __init__ (self):
		self.version = 0

class FakeVector:
	def __init__ (self):
		self.entries = [FakeVersion()]

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

# The python-memcache client is much like the Voldemort one, the main
# difference being that it stores cas versions itself instead of passing
# them back to us.
class StoreClient:
	def __init__ (self, store_name, bootstrap_urls):
		s_list = []
		for host, port in bootstrap_urls:
			# TBD: deal with port defaults properly
			s_list.append("%s:%d"%(host,11211))
		self.mc = memcache.Client(s_list)
		self.auto_mkfs = False
	def get (self, key):
		k2 = encode(key)
		data = self.mc.gets(k2)
		if not data:
			print "get(%s) FAILED" % k2
			return []
		#print "get(%s) => %d bytes" % (k2, len(data))
		return [(data,FakeVector())]
	def put (self, key, data, version):
		k2 = encode(key)
		if k2 in self.mc.cas_ids.keys():
			if not self.mc.cas(k2,data):
				print "cas(%s,%d) FAILED" % (k2, len(data))
				# TBD: figure out why
				self.mc.set(k2,data)
		else:
			if not self.mc.set(k2,data):
				print "set(%s,%d) FAILED" % (k2, len(data))
