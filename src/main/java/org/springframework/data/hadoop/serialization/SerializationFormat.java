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

/**
 * The interface represents a strategy to serialize objects to HDFS.
 * 
 * @see {@link ResourceSerializationFormat}
 * @see {@link SequenceFileFormat}
 * @see {@link AvroSequenceFileFormat}
 * @see {@link AvroFormat}
 */
public interface SerializationFormat<T> extends Closeable {

	// TODO: DON'T like it!
	// @Costin: is it OK to have such a method here?
	// - if OK: maybe open() is more appropriate name
	// - if not OK: how to pass the output? should it be part of the interface?
	void setOutputStream(OutputStream outputStream) throws IOException;

	void serialize(T object) throws IOException;

	/**
	 * Gets the filename extension for this kind of serialization format (such as '.avro', '.seqfile' or '.snappy').
	 * 
	 * @return The file extension including the '.' char or an empty/null string if not available.
	 */
	String getExtension();

}