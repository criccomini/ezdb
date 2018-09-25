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
package ezdb.rocksdb.util;

import java.util.AbstractMap;
import java.util.Map;
import java.util.NoSuchElementException;

import org.rocksdb.RocksIterator;

//implementation taken from leveldbjni
/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class RocksDBJniDBIterator implements DBIterator {

	private final RocksIterator iterator;

	public RocksDBJniDBIterator(RocksIterator iterator) {
		this.iterator = iterator;
	}

	public void close() {
		iterator.close();
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}

	public void seek(byte[] key) {
		iterator.seek(key);
	}

	public void seekToFirst() {
		iterator.seekToFirst();
	}

	public void seekToLast() {
		iterator.seekToLast();
	}

	public Map.Entry<byte[], byte[]> peekNext() {
		if (!iterator.isValid()) {
			throw new NoSuchElementException();
		}
		return new AbstractMap.SimpleImmutableEntry<byte[], byte[]>(
				iterator.key(), iterator.value());
	}

	public boolean hasNext() {
		return iterator.isValid();
	}

	public Map.Entry<byte[], byte[]> next() {
		Map.Entry<byte[], byte[]> rc = peekNext();
		iterator.next();
		return rc;
	}

	public boolean hasPrev() {
		if (!iterator.isValid())
			return false;
		iterator.prev();
		try {
			return iterator.isValid();
		} finally {
			if (iterator.isValid()) {
				iterator.next();
			} else {
				iterator.seekToFirst();
			}
		}
	}

	public Map.Entry<byte[], byte[]> peekPrev() {
		iterator.prev();
		try {
			return peekNext();
		} finally {
			if (iterator.isValid()) {
				iterator.next();
			} else {
				iterator.seekToFirst();
			}
		}
	}

	public Map.Entry<byte[], byte[]> prev() {
		Map.Entry<byte[], byte[]> rc = peekPrev();
		iterator.prev();
		return rc;
	}

}
