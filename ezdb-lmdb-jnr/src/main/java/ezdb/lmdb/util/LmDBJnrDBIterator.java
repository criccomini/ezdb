/*
 * Copyright (C) 2011, FuseSource Corp.  All rights reserved.
 *
 *     http://fusesource.com
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 * 
 *    * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *    * Neither the name of FuseSource Corp. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package ezdb.lmdb.util;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.lmdbjava.Cursor;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.GetOp;
import org.lmdbjava.Txn;

import ezdb.lmdb.EzLmDbComparator;

//implementation taken from leveldbjni
/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class LmDBJnrDBIterator implements DBIterator {
	
	private final Env<ByteBuffer> env;
	private final Dbi<ByteBuffer> dbi;
	private final Txn<ByteBuffer> txn;
	private final Cursor<ByteBuffer> cursor;
	private boolean valid = false;

	public LmDBJnrDBIterator(Env<ByteBuffer> env, Dbi<ByteBuffer> dbi) {
		this.env = env;
		this.dbi = dbi;
		this.txn = env.txnRead();
		this.cursor = dbi.openCursor(txn);
	}

	public void close() {
		cursor.close();
		txn.close(); 
	}
	
	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		close();
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}

	public void seek(byte[] key) {
		valid = cursor.get(DirectBuffers.wrap(key), GetOp.MDB_SET_RANGE);
	}

	public void seekToFirst() {
		valid = cursor.first();
	}

	public void seekToLast() {
		valid = cursor.last();
	}

	public Map.Entry<byte[], byte[]> peekNext() {
		if (!valid) {
			throw new NoSuchElementException();
		}
		return new AbstractMap.SimpleImmutableEntry<byte[], byte[]>(DirectBuffers.array(cursor.key()),
				DirectBuffers.array(cursor.val()));
	}

	public boolean hasNext() {
		return valid;
	}

	public Map.Entry<byte[], byte[]> next() {
		Map.Entry<byte[], byte[]> rc = peekNext();
		valid = cursor.next();
		return rc;
	}

	public boolean hasPrev() {
		if (!valid)
			return false;
		valid = cursor.prev();
		try {
			return valid;
		} finally {
			if (valid) {
				valid = cursor.next();
			} else {
				seekToFirst();
			}
		}
	}

	public Map.Entry<byte[], byte[]> peekPrev() {
		valid = cursor.prev();
		try {
			return peekNext();
		} finally {
			if (valid) {
				valid = cursor.next();
			} else {
				seekToFirst();
			}
		}
	}

	public Map.Entry<byte[], byte[]> prev() {
		Map.Entry<byte[], byte[]> rc = peekPrev();
		valid = cursor.prev();
		return rc;
	}

}
