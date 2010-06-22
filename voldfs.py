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

import errno
import os
import string
import struct
import sys
import traceback

import fuse
fuse.fuse_python_api = (0,2)

db = __import__(os.getenv("VOLDFS_DB","voldemort"))

import vfs_base
import vfs_dir

class NullObject:
	pass

class Collector:
	def __init__ (self):
		self.result = []
	def __call__ (self, name, ptr, hash):
		self.result.append((name,hash))

class VoldFS (fuse.Fuse):
	def __init__ (self, fs, root):
		fuse.Fuse.__init__(self,dash_s_do="setsingle")
		self.fs = fs
		self.root = root

	def fsinit (self):
		try:
			if self.fs.store.auto_mkfs:
				vfs_dir.mkdir(self.fs,self.root,0755)
		except:
			print "This storage type requires mkfs first"

	def getattr (self, path):
		ptr = vfs_dir.lookup(self.fs,self.root,path)
		if ptr == None:
			return -errno.ENOENT
		try:
			idata, vector = self.fs.get_inode(ptr)
			inode = struct.unpack(vfs_base.INODE_FMT,
				idata[:vfs_base.INODE_SZ])
		except:
			print "<<<%s>>>" % repr(ptr)
			traceback.print_exc()
			return -errno.EIO
		it = NullObject()
		it.st_mode = inode[0]
		it.st_ino = inode[1]
		it.st_dev = inode[2]
		it.st_nlink = inode[3]
		it.st_uid = inode[4]
		it.st_gid = inode[5]
		it.st_size = inode[6]
		it.st_atime = inode[7]
		it.st_mtime = inode[8]
		it.st_ctime = inode[9]
		return it

	def mkdir (self, path, mode):
		parts = path.split("/")
		parent = string.join(parts[:-1],"/")
		child = parts[-1]
		pptr = vfs_dir.lookup(self.fs,self.root,parent)
		if pptr == None:
			return -errno.ENOENT
		cptr = vfs_base.get_new_key()
		vfs_dir.mkdir(self.fs,cptr,mode)
		try:
			vfs_dir.link(self.fs,pptr,child,cptr)
		except vfs_dir.DupFileExc:
			return -errno.EEXIST

	def opendir (self, path):
		print "in opendir"

	def readdir (self, path, offset=0):
		print "in readdir(%s,0x%x)" % (path, offset)
		ptr = vfs_dir.lookup(self.fs,self.root,path)
		if ptr == None:
			raise IOError, "directory not found"
			return
		coll = Collector()
		vfs_dir.enum(self.fs,ptr,coll,offset)
		for name, hash in coll.result:
			x = fuse.Direntry(name)
			x.ino = hash
			yield x

	def create (self, path, flags, mode):
		print "in create(%s,0x%x,0x%x)" % (path, flags, mode)
		parts = path.split("/")
		parent = string.join(parts[:-1],"/")
		child = parts[-1]
		pptr = vfs_dir.lookup(self.fs,self.root,parent)
		if pptr == None:
			return -errno.ENOENT
		cptr = vfs_base.get_new_key()
		self.fs.create_inode(cptr,mode)
		try:
			vfs_dir.link(self.fs,pptr,child,cptr)
		except vfs_dir.DupFileExc:
			return -errno.EEXIST

	def write (self, path, buf, offset, fh=None):
		ptr = vfs_dir.lookup(self.fs,self.root,path)
		if ptr == None:
			return -errno.ENOENT
		return self.fs.put_data(ptr,offset,buf)

	def read (self, path, length, offset, fh=None):
		ptr = vfs_dir.lookup(self.fs,self.root,path)
		if ptr == None:
			return -errno.ENOENT
		total = ''
		while len(total) < length:
			new_data = self.fs.get_data(ptr,offset,length)
			if not len(new_data):
				break
			total += new_data
			offset += len(new_data)
		return total

	def unlink (self, path):
		print "in unlink(%s)" % path
		parts = path.split("/")
		parent = string.join(parts[:-1],"/")
		child = parts[-1]
		pptr = vfs_dir.lookup(self.fs,self.root,parent)
		if pptr == None:
			return -errno.ENOENT
		return vfs_dir.unlink(self.fs,pptr,child)

	def chmod (self, path, mode):
		print "in chmod(%s,0x%x)" % (path, mode)

	def chown (self, path, user, group):
		print "in chown(%s,%d,%d)" % (path, user, group)

	def truncate (self, path, len):
		print "in truncate(%s,%d)" % (path, len)

	def utime (self, path, times):
		print "in utimes(%s)" % path

	def statfs (self):
		print "in statfs"
		svf = NullObject()
		svf.f_bsize   = vfs_base.BLOCK_SZ
		svf.f_frsize  = vfs_base.BLOCK_SZ
		svf.f_blocks  = 1000000
		svf.f_bfree   = 800000
		svf.f_bavail  = 100000
		svf.f_files   = 1000
		svf.f_ffree   = 10000
		svf.f_favail  = 1000
		svf.f_flag    = 0
		svf.f_namemax = vfs_dir.MAX_NAME_LEN
		return svf
	
if __name__ == "__main__":
	# I don't have enough patience for the hairball that is FUSE option
	# parsing.  I have the actual arguments, I'll extract the ones I
	# want myself thankyouverymuch.
	i = 0
	while i < len(sys.argv):
		this_arg = sys.argv[i]
		if (len(this_arg) >= 2) and (this_arg[:2] == "-0"):
			if len(this_arg) > 2:
				arg_text = this_arg[2:]
			else:
				arg_text = sys.argv[i+1]
				del sys.argv[i+1]
			del sys.argv[i]
			print "my opts = %s" % arg_text
			for opt in arg_text.split(","):
				key, value = opt.split("=")
				if key == "host":
					print "<<host = %s>>" % value
				elif key == "port":
					print "<<port = %s>>" % value
				elif key == "db":
					print "<<db = %s>>" % value
				else:
					print "unknown key/value %s" % opt
		else:
			i += 1
	store = db.StoreClient("test",[("localhost",6666)])
	fs = vfs_base.FS(store)
	vfs = VoldFS(fs,"root")
	vfs.parse()
	vfs.main()

