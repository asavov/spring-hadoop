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
 * The interface is responsible to create serizalization format instances. Different implementations might provide
 * different serialization mechanisms, such as Avro or SeqFile.
 * 
 * @param <T> The type of objects serialized by the {@link SerializationFormat} instance returned by this creator.
 * 
 * @see {@link ResourceSerializationFormatCreator}
 * @see {@link SequenceFileFormatCreator}
 * @see {@link AvroSequenceFileFormatCreator}
 * @see {@link AvroFormatCreator}
 * 
 * @author Alex Savov
 */
public interface SerializationFormatCreator<T> {

	/**
	 * Creates a serialization format that writes to the specified <code>OutputStream</code>.
	 * 
	 * <p>
	 * Note: The output stream is closed upon {@link SerializationFormat#close() closing} the
	 * <code>SerializationFormat</code> instance.
	 * 
	 * @param output The output stream to which created serialization format should write.
	 * 
	 * @return Serialization format that writes to the specified <code>OutputStream</code>.
	 */
	SerializationFormat<T> createSerializationFormat(OutputStream output);

	/**
	 * Gets the filename extension for this kind of serialization format (such as '.avro', '.seqfile' or '.snappy').
	 * 
	 * <p>
	 * It is advisable but not obligatory the output stream passed to {@link #createSerializationFormat(OutputStream)}
	 * to point a file resource with that extension.
	 * 
	 * @return The file extension including the '.' char or an <code>empty/null</code> string if not available.
	 */
	String getExtension();

}