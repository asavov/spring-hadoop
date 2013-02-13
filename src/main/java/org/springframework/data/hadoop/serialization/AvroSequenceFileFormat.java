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

import org.apache.avro.Schema;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.io.SequenceFile;

/**
 * Serialization format writing POJOs in <code>Avro</code> schema using Hadoop {@link SequenceFile} serialization
 * framework.
 * 
 * @author Alex Savov
 */
public class AvroSequenceFileFormat<T> extends AbstractSequenceFileFormat<T> {

	public AvroSequenceFileFormat(Class<T> objectsClass) {
		super(objectsClass);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void afterPropertiesSet() throws Exception {

		super.afterPropertiesSet();

		// Reflective Avro schema of key class
		Schema keySchema = ReflectData.get().getSchema(getSerializationKeyProvider().getKeyClass(objectsClass));

		AvroSerialization.setKeyWriterSchema(getConfiguration(), keySchema);

		// Reflective Avro schema of value class
		Schema valueSchema = ReflectData.get().getSchema(objectsClass);

		AvroSerialization.setValueWriterSchema(getConfiguration(), valueSchema);

		registerSeqFileSerialization(AvroSerialization.class);
	}

	/**
	 * Writes POJOs in <code>Avro</code> schema using Hadoop {@link SequenceFile} serialization framework.
	 */
	@Override
	protected SerializationWriterSupport createWriter(OutputStream output) {

		return new AbstractSequenceFileWriter(output) {

			/**
			 * @return <code>AvroKey.class</code> is always used to write the key as Avro data
			 */
			@Override
			protected final Class<?> getKeyClass() {
				return AvroKey.class;
			}

			/**
			 * @return new <code>AvroKey</code> wrapper around the key of core object
			 */
			@SuppressWarnings({ "unchecked", "rawtypes" })
			@Override
			protected Object getKey(T object) {
				return new AvroKey(getSerializationKeyProvider().getKey(object));
			}

			/**
			 * @return <code>AvroValue.class</code> is always used to write the object as Avro data
			 */
			@Override
			protected final Class<?> getValueClass() {
				return AvroValue.class;
			}

			/**
			 * @return new <code>AvroValue</code> wrapper around the core object
			 */

			@Override
			protected AvroValue<T> getValue(T object) {
				return new AvroValue<T>(object);
			}
		};
	}

	/**
	 * Reads POJOs in <code>Avro</code> schema using Hadoop {@link SequenceFile} serialization framework.
	 */
	@Override
	protected SerializationReaderSupport createReader(String location) {

		return new AbstractSequenceFileReader(location) {

			@SuppressWarnings("unchecked")
			@Override
			protected T getValue(Object object) {
				return ((AvroWrapper<T>) object).datum();
			}
		};
	}
}