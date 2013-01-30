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

import java.io.IOException;

/**
 * Experimental class by analogy with other <code>xxxOperations</code> classes. Needs to validate if it's applicable and
 * usable in HDFS serialization context/API.
 * 
 * @author Alex Savov
 */
public interface SerializationFormatOperations {

	public static interface SerializationFormatCallback<T> {

		void doInSerializationFormat(SerializationFormat<T> serializationFormat) throws IOException;

	}

	<T> void execute(String destination, SerializationFormatCallback<T> action) throws IOException;

}
