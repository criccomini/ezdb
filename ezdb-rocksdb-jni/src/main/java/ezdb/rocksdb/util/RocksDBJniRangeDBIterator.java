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

import java.util.NoSuchElementException;

import org.rocksdb.RocksIterator;

import ezdb.serde.Serde;
import ezdb.table.range.RawRangeTableRow;

//implementation taken from leveldbjni
/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class RocksDBJniRangeDBIterator<H, R, V> implements EzDBRangeIterator<H, R, V> {

	private final RocksIterator iterator;
	private final Serde<H> hashKeySerde;
	private final Serde<R> rangeKeySerde;
	private final Serde<V> valueSerde;

	public RocksDBJniRangeDBIterator(final RocksIterator iterator, final Serde<H> hashKeySerde, final Serde<R> rangeKeySerde,
			final Serde<V> valueSerde) {
		this.iterator = iterator;
		this.hashKeySerde = hashKeySerde;
		this.rangeKeySerde = rangeKeySerde;
		this.valueSerde = valueSerde;
	}

	@Override
	public void close() {
		iterator.close();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void seek(final byte[] key) {
		iterator.seek(key);
	}

	@Override
	public void seekToFirst() {
		iterator.seekToFirst();
	}

	@Override
	public void seekToLast() {
		iterator.seekToLast();
	}

	@Override
	public RawRangeTableRow<H, R, V> peekNext() {
		if (!iterator.isValid()) {
			throw new NoSuchElementException();
		}
		return RawRangeTableRow.valueOfBytes(iterator.key(), iterator.value(), hashKeySerde, rangeKeySerde, valueSerde);
	}

	@Override
	public byte[] peekNextKey() {
		if (!iterator.isValid()) {
			throw new NoSuchElementException();
		}
		return iterator.key();
	}

	@Override
	public boolean hasNext() {
		return iterator.isValid();
	}

	@Override
	public RawRangeTableRow<H, R, V> next() {
		final RawRangeTableRow<H, R, V> rc = peekNext();
		iterator.next();
		return rc;
	}

	@Override
	public byte[] nextKey() {
		final byte[] rc = peekNextKey();
		iterator.next();
		return rc;
	}

	@Override
	public boolean hasPrev() {
		if (!iterator.isValid()) {
			return false;
		}
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

	@Override
	public RawRangeTableRow<H, R, V> peekPrev() {
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

	@Override
	public byte[] peekPrevKey() {
		iterator.prev();
		try {
			return peekNextKey();
		} finally {
			if (iterator.isValid()) {
				iterator.next();
			} else {
				iterator.seekToFirst();
			}
		}
	}

	@Override
	public RawRangeTableRow<H, R, V> prev() {
		final RawRangeTableRow<H, R, V> rc = peekPrev();
		iterator.prev();
		return rc;
	}

}
