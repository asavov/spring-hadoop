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
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;

/**
 * Serialization format for writing POJOs using <code>Avro</code> serialization.
 * 
 * @author Alex Savov
 */
public class AvroFormatCreator<T> extends SerializationFormatCreatorSupport<T> {

	/* The class of the objects that are serialized by this format. */
	protected final Class<T> objectsClass;

	/**
	 * @param objectsClass The class of the objects that are serialized by this format.
	 */
	public AvroFormatCreator(Class<T> objectsClass) {
		this.objectsClass = objectsClass;
	}

	public SerializationFormat<T> createSerializationFormat(final OutputStream output) {

		return new SerializationFormatSupport() {

			/* Native Avro writer. */
			DataFileWriter<T> writer;

			@Override
			protected Closeable doOpen() throws IOException {
				Schema schema = ReflectData.get().getSchema(objectsClass);

				writer = new DataFileWriter<T>(new ReflectDatumWriter<T>(schema));

				writer.setCodec(CompressionUtils.getAvroCompression(getCompressionAlias()));

				writer.create(schema, output);

				return writer;
			}

			@Override
			protected void doSerialize(T object) throws IOException {
				writer.append(object);
			}
		};
	}

	/**
	 * @return <b>.avro</b> is the default file extension for Avro serialization.
	 */
	@Override
	public String getExtension() {
		return ".avro";
	}

}
