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

import org.apache.hadoop.io.NullWritable;

/**
 * An implementation of {@link SerializationKeyProvider} returning {@link NullWritable} key for every object.
 * 
 * @author Alex Savov
 */
public class NullWritableSerializationKeyProvider implements SerializationKeyProvider {

	public static final NullWritableSerializationKeyProvider INSTANCE = new NullWritableSerializationKeyProvider();

	/**
	 * @return The singleton <code>NullWritable</code> as returned by {@link NullWritable#get()} method.
	 */
	public NullWritable getKey(Object object) {
		return NullWritable.get();
	}

	/**
	 * @return <code>NullWritable.class</code> for every object class
	 */
	public Class<NullWritable> getKeyClass(Class<?> objectClass) {
		return NullWritable.class;
	}
}