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

import org.springframework.core.serializer.Serializer;

/**
 * The interface represents a strategy to serialize objects to HDFS.
 * 
 * @see {@link ResourceSerializationFormat}
 * @see {@link SequenceFileFormat}
 * @see {@link AvroSequenceFileFormat}
 * @see {@link AvroFormat}
 */
public interface SerializationFormat<T> extends Serializer<T> {
	// TODO: think about that: extends FactoryBean<OutputStream>

	/**
	 * Gets the filename extension for this kind of serialization format (such as '.avro', '.seqfile' or '.snappy').
	 * 
	 * @return The file extension including the '.' char or an empty/null string if not available.
	 */
	public abstract String getExtension();

}