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

/**
 * The class provides support needed by {@link SerializationFormat} implementations that support compression. This
 * feature is useful for writing large objects.
 * 
 * @author Alex Savov
 */
public abstract class CompressedSerializationFormat<T> implements SerializationFormat<T> {

	private String compressionAlias;

	/**
	 * Sets the compression alias for this <code>SerializationFormat</code>. It's up to the implementation to resolve
	 * the alias to the actual compression algorithm.
	 * 
	 * @param codecAlias The compression alias to use.
	 */
	public void setCompressionAlias(String compressionAlias) {
		this.compressionAlias = compressionAlias;
	}

	public String getCompressionAlias() {
		return compressionAlias;
	}

}