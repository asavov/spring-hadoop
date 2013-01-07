/*
 * Copyright 2004-2012 the original author or authors.
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
package org.springframework.data.hadoop.fs;

import org.apache.avro.Schema;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.io.SequenceFile;

/**
 * Serialization format for writing POJOs in <code>Avro</code> schema using Hadoop {@link SequenceFile} serialization
 * framework.
 * 
 * @author Alex Savov
 */
public class AvroSequenceFileFormat extends SequenceFileFormatSupport {

	@Override
	@SuppressWarnings("unchecked")
	protected <T> void doInit(Iterable<? extends T> objects, Class<T> objectsClass, HdfsResource hdfsResource) {

		// Reflective Avro schema of key class
		Schema keySchema = ReflectData.get().getSchema(serializationKeyProvider.getKeyClass(objectsClass));

		AvroSerialization.setKeyWriterSchema(getConfiguration(), keySchema);

		// Reflective Avro schema of value class
		Schema valueSchema = ReflectData.get().getSchema(objectsClass);

		AvroSerialization.setValueWriterSchema(getConfiguration(), valueSchema);

		register(AvroSerialization.class);
	}

	/**
	 * @return <code>AvroKey.class</code> is always used to write Avro data
	 */
	protected final Class<?> getKeyClass(Class<?> objectClass) {
		return AvroKey.class;
	}

	/**
	 * @return new <code>AvroKey</code> wrapper around core object key
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected Object getKey(Object object) {
		return new AvroKey(serializationKeyProvider.getKey(object));
	}

	/**
	 * @return <code>AvroValue.class</code> is always used to write Avro data
	 */
	protected final Class<?> getValueClass(Class<?> objectClass) {
		return AvroValue.class;
	}

	/**
	 * @return new <code>AvroValue</code> wrapper around core object
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected Object getValue(Object object) {
		return new AvroValue(object);
	}
}