/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.hadoop.serialization;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.io.IOUtils;
import org.springframework.util.Assert;

/**
 * The class provides support needed by {@link SerializationFormat} implementations that support compression. This
 * feature is useful for writing large objects.
 * 
 * @author Alex Savov
 */
public abstract class SerializationFormatSupport<T> implements SerializationFormat<T> {

	/* This property is publicly configurable. */
	private String compressionAlias;

	/* This property is publicly configurable. */
	private OutputStream outputStream;

	protected boolean open = false;

	protected Closeable nativeResource;

	/**
	 * Sets the compression alias for this <code>SerializationFormat</code>. It's up to the implementation to resolve
	 * the alias to the actual compression algorithm.
	 * 
	 * @param codecAlias The compression alias to use.
	 */
	public void setCompressionAlias(String compressionAlias) {
		this.compressionAlias = compressionAlias;
	}

	protected String getCompressionAlias() {
		return compressionAlias;
	}

	/**
	 * Sets the output stream to serialize the content to.
	 * 
	 * @param outputStream The output stream to use.
	 */
	public void setOutputStream(OutputStream outputStream) {
		this.outputStream = outputStream;
	}

	protected OutputStream getOutputStream() {
		return outputStream;
	}

	public void serialize(T source) throws IOException {

		open();

		doSerialize(source);
	}

	protected void open() throws IOException {
		if (!open) {
			Assert.notNull(getOutputStream(), "A non-null OutputStream is required.");

			nativeResource = doOpen();

			open = true;
		}
	}

	protected abstract Closeable doOpen() throws IOException;

	protected abstract void doSerialize(T source) throws IOException;

	@Override
	public void close() throws IOException {
		IOUtils.closeStream(nativeResource);
		nativeResource = null;
		open = false;
	}

}