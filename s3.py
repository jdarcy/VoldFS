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

import boto
import os

class FakeVersion:
	def __init__ (self):
		self.version = 0

class FakeVector:
	def __init__ (self, v):
		self.value = v
		self.entries = [FakeVersion()]

# Memcached keys are not binary-safe, so we convert to a strict text form.
def encode (istr):
	ostr = ""
	for c in istr:
		ostr += '%02x' % ord(c)
	return ostr

class StoreClient:
	def __init__ (self, store_name, bootstrap_urls):
		self.auto_mkfs = False
		self.key = os.getenv("VOLDFS_KEY")
		self.secret = os.getenv("VOLDFS_SECRET")
		self.bucket = os.getenv("VOLDFS_BUCKET")
		self.conn = boto.connect_s3(self.key,self.secret)
		self.bucket = self.conn.get_bucket(self.bucket)
	def get (self, key):
		a_key = self.bucket.new_key(encode(key))
		data = a_key.get_contents_as_string()
		return [[data,FakeVector(0)]]
	def put (self, key, data, version):
		a_key = self.bucket.new_key(encode(key))
		a_key.set_contents_from_string(data)
