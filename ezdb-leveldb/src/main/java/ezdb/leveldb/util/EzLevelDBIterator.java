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
package ezdb.leveldb.util;

import java.util.NoSuchElementException;

import org.iq80.leveldb.iterator.ExtendedDBIteratorAdapter;
import org.iq80.leveldb.util.Slice;

import ezdb.RawTableRow;
import ezdb.serde.Serde;

//implementation taken from leveldbjni
/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class EzLevelDBIterator<H, R, V> implements EzDBIterator<H, R, V> {

	private final ExtendedDBIteratorAdapter iterator;
	private final Serde<H> hashKeySerde;
	private final Serde<R> rangeKeySerde;
	private final Serde<V> valueSerde;
	private boolean valid = false;

	public EzLevelDBIterator(final ExtendedDBIteratorAdapter iterator, final Serde<H> hashKeySerde,
			final Serde<R> rangeKeySerde, final Serde<V> valueSerde) {
		this.iterator = iterator;
		this.hashKeySerde = hashKeySerde;
		this.rangeKeySerde = rangeKeySerde;
		this.valueSerde = valueSerde;
	}

	@Override
	public void close() {
		iterator.close();
	}

	@SuppressWarnings("deprecation")
	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		close();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void seek(final Slice key) {
		valid = iterator.seek(key);
	}

	@Override
	public void seekToFirst() {
		valid = iterator.seekToFirst();
	}

	@Override
	public void seekToLast() {
		valid = iterator.seekToLast();
	}

	@Override
	public RawTableRow<H, R, V> peekNext() {
		if (!valid) {
			throw new NoSuchElementException();
		}
		return Slices.newRawTableRow(iterator.getKey(), iterator.getValue(), hashKeySerde, rangeKeySerde, valueSerde);
	}

	@Override
	public boolean hasNext() {
		return valid;
	}

	@Override
	public RawTableRow<H, R, V> next() {
		final RawTableRow<H, R, V> rc = peekNext();
		valid = iterator.next();
		return rc;
	}

	@Override
	public Slice nextKey() {
		final Slice rc = peekNextKey();
		valid = iterator.next();
		return rc;
	}

	@Override
	public boolean hasPrev() {
		if (!valid) {
			return false;
		}
		valid = iterator.prev();
		try {
			return valid;
		} finally {
			if (valid) {
				valid = iterator.next();
			} else {
				seekToFirst();
			}
		}
	}

	@Override
	public RawTableRow<H, R, V> peekPrev() {
		valid = iterator.prev();
		try {
			return peekNext();
		} finally {
			if (valid) {
				valid = iterator.next();
			} else {
				seekToFirst();
			}
		}
	}

	@Override
	public RawTableRow<H, R, V> prev() {
		final RawTableRow<H, R, V> rc = peekPrev();
		valid = iterator.prev();
		return rc;
	}

	@Override
	public Slice peekNextKey() {
		if (!valid) {
			throw new NoSuchElementException();
		}
		return iterator.getKey();
	}

	@Override
	public Slice peekPrevKey() {
		valid = iterator.prev();
		try {
			return peekNextKey();
		} finally {
			if (valid) {
				valid = iterator.next();
			} else {
				seekToFirst();
			}
		}
	}

}
