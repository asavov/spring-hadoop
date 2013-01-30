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

import static org.apache.commons.io.IOUtils.closeQuietly;

import java.io.Closeable;
import java.io.IOException;

import org.apache.commons.io.IOUtils;

/**
 * The class provides common support needed by {@link SerializationFormatCreator} implementations.
 * 
 * @author Alex Savov
 */
public abstract class SerializationFormatCreatorSupport<T> implements SerializationFormatCreator<T> {

	/* This property is publicly configurable. */
	private String compressionAlias;

	/**
	 * Sets the compression alias for the <code>SerializationFormat</code>s created by this class. It's up to the
	 * implementation to resolve the alias to the actual compression algorithm.
	 * 
	 * @param compressionAlias The compression alias to use.
	 */
	public void setCompressionAlias(String compressionAlias) {
		this.compressionAlias = compressionAlias;
	}

	protected String getCompressionAlias() {
		return compressionAlias;
	}

	/**
	 * A template class to be extended by SerializationFormatCreatorSupport descendants and returned by their
	 * createSerializationFormat method.
	 */
	protected abstract class SerializationFormatSupport implements SerializationFormat<T> {

		/* Indicates whether this serialization format has been used for writing. */
		protected boolean isOpen = false;

		/* The native resource used by this serialization format that should be released upon close(). */
		protected Closeable nativeResource;

		@Override
		public void serialize(T source) throws IOException {

			// Lazy open serialization format upon first write
			open();

			// Delegate to core serialization
			doSerialize(source);
		}

		/**
		 * Close the native resource returned by {@link #doOpen()}.
		 */
		@Override
		public void close() throws IOException {
			closeQuietly(nativeResource);
			nativeResource = null;
			isOpen = false;
		}

		/**
		 * Lazy open serialization format for writing.
		 * @throws IOException
		 */
		protected void open() throws IOException {
			if (!isOpen) {
				nativeResource = doOpen();

				isOpen = true;
			}
		}

		/**
		 * A hook method to be implemented by descendats to execute custom open logic.
		 * @return The underlying native resource (as Closeable) to be released by {@link #close()}.
		 * @throws IOException
		 */
		protected abstract Closeable doOpen() throws IOException;

		/**
		 * Here goes core serialization logic.
		 * @param source The object to write.
		 * @throws IOException
		 */
		protected abstract void doSerialize(T source) throws IOException;

	}

}