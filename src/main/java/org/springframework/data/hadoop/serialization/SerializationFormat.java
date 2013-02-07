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

import java.io.OutputStream;

/**
 * The interface represents Hadoop specific serialization format, such as Avro or SequenceFile.
 * 
 * @param <T> The type of objects (de)serialized by this serialization format.
 * 
 * @see {@link ResourceSerializationFormat}
 * @see {@link SequenceFileFormat}
 * @see {@link AvroSequenceFileFormat}
 * @see {@link AvroFormat}
 * 
 * @author Alex Savov
 */
public interface SerializationFormat<T> {

	/**
	 * Creates a serialization writer that writes to the specified <code>OutputStream</code>.
	 * 
	 * <p>
	 * Note: The output stream is closed upon {@link SerializationWriter#close() closing} the
	 * <code>SerializationWriter</code> instance.
	 * 
	 * @param output The output stream to write to.
	 * 
	 * @return A Writer that writes to the specified <code>OutputStream</code>.
	 */
	SerializationWriter<T> getWriter(OutputStream output);

	/**
	 * Creates a serialization reader that reads from the specified location.
	 * 
	 * @param location The location to read from.
	 * 
	 * @return A Reader that reads from the specified location.
	 */
	SerializationReader<T> getReader(String location);

	/**
	 * Gets the filename extension for this kind of serialization format (such as '.avro', '.seqfile' or '.snappy').
	 * 
	 * <p>
	 * It is advisable but not obligatory the output stream passed to {@link #getWriter(OutputStream)} to point a
	 * resource with that extension.
	 * 
	 * @return The file extension including the '.' char or an <code>empty</code> string if not available.
	 */
	String getExtension();

}