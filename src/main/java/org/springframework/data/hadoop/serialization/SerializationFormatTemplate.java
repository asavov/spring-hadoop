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

import static org.apache.hadoop.io.IOUtils.closeStream;

import java.io.IOException;

/**
 * Experimental class by analogy with other <code>xxxTemplate</code> classes. Needs to validate if it's applicable and
 * usable in HDFS serialization context/API.
 * 
 * @author Alex Savov
 */
public class SerializationFormatTemplate implements SerializationFormatOperations {

	protected SerializationWriterObjectFactory sfObjectFactory;

	public SerializationFormatTemplate(SerializationWriterObjectFactory sfObjectFactory) {
		this.sfObjectFactory = sfObjectFactory;
	}

	@Override
	public <T> void write(String destination, SerializationWriterCallback<T> action) throws IOException {

		sfObjectFactory.setDestination(destination);

		@SuppressWarnings("unchecked")
		SerializationWriter<T> serialization = (SerializationWriter<T>) sfObjectFactory.getObject();

		try {
			action.doInSerializationFormat(serialization);
		} finally {
			closeStream(serialization);
		}
	}

}
