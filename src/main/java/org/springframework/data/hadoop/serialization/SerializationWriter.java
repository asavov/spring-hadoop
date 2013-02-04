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

/**
 * A strategy interface encapsulating the logic to serialize objects to HDFS.
 * 
 * <p>
 * Instances of this interface are created by {@link SerializationFormat}.
 * 
 * @see {@link SerializationWriter}
 * 
 * @author Alex Savov
 */
public interface SerializationWriter<T> extends Closeable {

	/**
	 * Writes an object in this serialization format.
	 * 
	 * @param object The object to write.
	 * 
	 * @throws IOException in case of errors writing to the stream
	 */
	void write(T object) throws IOException;

}