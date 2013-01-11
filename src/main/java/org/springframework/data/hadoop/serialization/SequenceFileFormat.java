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

import java.io.Serializable;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.JavaSerialization;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.WritableSerialization;

/**
 * Serialization format for writing Java classes that are accepted by a {@link Serialization} registered with the
 * {@link SerializationFactory} using Hadoop {@link SequenceFile} serialization framework. By default Hadoop comes with
 * serialization support for {@link Serializable} and {@link Writable} classes.
 * 
 * @author Alex Savov
 */
public class SequenceFileFormat<T> extends AbstractSequenceFileFormat<T> {

	public SequenceFileFormat(Class<T> objectsClass) {
		super(objectsClass);
	}

	/**
	 * Adds <code>WritableSerialization</code> and <code>JavaSerialization</code> schemes to Hadoop configuration, so
	 * {@link SerializationFactory} instances constructed from the given configuration will be aware of it.
	 */
	@SuppressWarnings("unchecked")
	public void afterPropertiesSet() throws Exception {

		super.afterPropertiesSet();

		registerSeqFileSerialization(getConfiguration(), WritableSerialization.class, JavaSerialization.class);
	}

	protected Class<?> getKeyClass(Class<?> objectClass) {
		return getSerializationKeyProvider().getKeyClass(objectClass);
	}

	protected Object getKey(Object object) {
		return getSerializationKeyProvider().getKey(object);
	}

	protected Class<?> getValueClass(Class<?> objectClass) {
		return objectClass;
	}

	protected Object getValue(Object object) {
		return object;
	}
}