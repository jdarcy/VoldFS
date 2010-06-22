#!/usr/bin/python

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

import os
import struct
import sys
import traceback

db = __import__(os.getenv("VOLDFS_DB","voldemort"))

import vfs_base
import vfs_dir

s = db.StoreClient("test",[("localhost",6666)])
fs = vfs_base.FS(s)
status = "OK"

### TEST SET 1: create and then do single-level lookups for 1000 files.

vfs_dir.mkdir(fs,"root",0755)

for i in range(1000):
	path = "file%d" % i
	key = struct.pack(vfs_base.PTR_FMT,1,1,i+100)
	try:
		vfs_dir.link(fs,"root",path,key)
		print "add(%s) OK" % path
	except:
		traceback.print_exc()
		print "add(%s) FAILED" % path
		status = "FAILED"

print "adding file0 AGAIN"
key = struct.pack(vfs_base.PTR_FMT,1,1,0)
vfs_dir.link(fs,"root","file0",key)

for i in range(1000):
	path = "/file%d" % i
	try:
		ptr = vfs_dir.lookup(fs,"root",path)
		if not ptr:
			raise RuntimeError, "lookup failed"
		print "lookup(%s) OK" % path
	except:
		traceback.print_exc()
		print "lookup(%s) FAILED" % path
		status = "FAILED"

if status != "OK":
	print "status = <<%s>>, skipping other tests" % status
	raise SystemExit

### TEST SET 2: test multi-level lookup

vfs_dir.mkdir(fs,"multi",0755)

# Create a couple of levels of directories.
sd1_key = vfs_base.get_new_key()
vfs_dir.mkdir(fs,sd1_key,0755)
vfs_dir.link(fs,"multi","a",sd1_key)

sd2_key = vfs_base.get_new_key()
vfs_dir.mkdir(fs,sd2_key,0755)
vfs_dir.link(fs,sd1_key,"b",sd2_key)

# Create an actual file in the lower subdirectory.
a_key = vfs_base.get_new_key()
a_file = fs.create_inode(a_key,0644)
fs.put_data(a_key,0,"hello world")
vfs_dir.link(fs,sd2_key,"cde",a_key)

print "Setup complete, let's see if we can read it back"

another_key = vfs_dir.lookup(fs,"multi","/a/b/cde")
data = fs.get_data(another_key,0,1024)
print repr(data)

if status != "OK":
	print "status = <<%s>>, skipping other tests" % status
	raise SystemExit

### TEST SET 3: create and then enumerate 1000 files

vfs_dir.mkdir(fs,"enum",0755)

for i in range(1000):
	path = "file%d" % i
	key = struct.pack(vfs_base.PTR_FMT,1,2,i)
	try:
		vfs_dir.link(fs,"enum",path,key)
	except:
		traceback.print_exc()
		print "enum add(%s) FAILED" % path
		raise SystemExit

class enum_test:
	def __init__ (self):
		self.last_seen = 0
		self.this_time = 0
		self.seen = []
	def test_cb (self, name, path, idx):
		self.last_seen = idx
		print "enum %s => %s (0x%x)" % (repr(name), repr(path), idx)
		self.this_time += 1
		self.seen.append(name)
		if self.this_time % 12:
			return 0
		self.this_time = 0
		return 1

et = enum_test()
while not vfs_dir.enum(fs,"enum",et.test_cb,et.last_seen):
	print "got partial result, last_seen = 0x%x" % et.last_seen

if "." not in et.seen:
	print ". missing from result list"
	status = "FAILED"
if ".." not in et.seen:
	print ". missing from result list"
	status = "FAILED"
for i in range(1000):
	path = "file%d" % i
	if path not in et.seen:
		print "%s missing from result list" % path
		status = "FAILED"
if len(et.seen) != 1002:
	print "wrong size %d for result list" % len(et.seen)
	status = "FAILED"

if status != "OK":
	print "status = <<%s>>, skipping other tests" % status
	raise SystemExit

# Test deletion.
vfs_dir.unlink(fs,"enum","file0")
a_key = vfs_dir.lookup(fs,"enum","file0")
if a_key != None:
	print "got key %s for deleted file" % repr(a_key)
	status = "FAILED"

print "status = %s" % status
