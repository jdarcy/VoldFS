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
import random
import struct

db = __import__(os.getenv("VOLDFS_DB","voldemort"))

import vfs_base

status = "OK"

tests = []
cursor = 0
for head_bytes in 0, 100:
	offset = (vfs_base.BLOCK_SZ - head_bytes) % vfs_base.BLOCK_SZ
	length = head_bytes
	for middle in 0, 1, 2:
		mlen = length + middle * vfs_base.BLOCK_SZ
		for tail_bytes in 0, 100:
			tlen = mlen + tail_bytes
			if tlen:
				#input = (head_bytes, middle, tail_bytes)
				output = (offset+cursor, tlen)
				tests.append(output)
				cursor += vfs_base.BLOCK_SZ * 13

store = db.StoreClient("test",[("localhost",6666)])
fs = vfs_base.FS(store)
fs.create_inode("test",0755)

random.shuffle(tests)
for t in tests:
	print "WRITING offset = %u, length = %u" % t
	offset, length = t
	bstr = "<%u>" % offset
	estr = "<%u>" % length
	blank = length - len(bstr) - len(estr)
	buffer = bstr + struct.pack("%ds"%blank,"") + estr
	fs.put_data("test",offset,buffer)
	#fs.dump("test")

def read_loop (name, offset, length):
	global fs
	result = ""
	so_far = 0
	while so_far < length:
		buffer = fs.get_data(name,offset+so_far,length-so_far)
		if len(buffer) <= 0:
			break
		result += buffer
		so_far = len(result)
	return result
	
random.shuffle(tests)
for t in tests:
	print "READING offset = %u, length = %u" % t
	offset, length = t
	bstr = "<%u>" % offset
	estr = "<%u>" % length
	buffer = read_loop("test",offset,length)
	if len(buffer) != length:
		print "  WRONG LENGTH %u" % len(buffer) 
		status = "FAILED"
		continue
	if buffer[:len(bstr)] != bstr:
		print "  WRONG HEADER %s" % repr(buffer[:len(bstr)])
		status = "FAILED"
		continue
	if buffer[-len(estr):] != estr:
		print "  WRONG TRAILER %s" % repr(buffer[-len(estr)])
		status = "FAILED"
		continue
	print "  buffer OK"

fs.create_inode("overlap",0755)

# Test overlap within an embedded block.
fs.put_data("overlap",997,"aaabbb")
fs.put_data("overlap",994,"cccddd")
fs.put_data("overlap",1000,"eeefff")
if fs.get_data("overlap",997,6) == "dddeee":
	print "first overlap test OK"
else:
	print "WRONG DATA on first overlap test"
	status = "FAILED"

# Test overlap within a non-embedded block.
fs.put_data("overlap",9997,"ggghhh")
fs.put_data("overlap",9994,"iiijjj")
fs.put_data("overlap",10000,"kkklll")
if fs.get_data("overlap",9997,6) == "jjjkkk":
	print "second overlap test OK"
else:
	print "WRONG DATA on second overlap test"
	status = "FAILED"

# Test overlap at a block boundary.
fs.put_data("overlap",16381,"mmmnnn")
fs.put_data("overlap",16378,"oooppp")
fs.put_data("overlap",16384,"qqqrrr")
# Need a loop here because reads stop at block boundaries.
data = ""
while len(data) < 6:
	data += fs.get_data("overlap",16381+len(data),6-len(data))
if data == "pppqqq":
	print "third overlap test OK"
else:
	print "WRONG DATA (%s) on third overlap test" % repr(data)
	status = "FAILED"

# Go back and check that the embedded block survived expansion.
if fs.get_data("overlap",997,6) == "dddeee":
	print "fourth overlap test OK"
else:
	print "WRONG DATA on fourth overlap test"
	status = "FAILED"

print "status = %s" % status
