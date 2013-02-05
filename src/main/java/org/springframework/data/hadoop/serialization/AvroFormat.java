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
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.springframework.core.io.Resource;

/**
 * Serialization formats writing POJOs using <code>Avro</code> serialization.
 * 
 * @author Alex Savov
 */
public class AvroFormat<T> extends SerializationFormatSupport<T> {

	/* The class of the objects that are serialized by this format. */
	protected final Class<T> objectsClass;

	/**
	 * @param objectsClass The class of the objects that are serialized by this format.
	 */
	public AvroFormat(Class<T> objectsClass) {
		this.objectsClass = objectsClass;
	}

	/**
	 * Writes POJOs using <code>Avro</code> serialization.
	 */
	public SerializationWriter<T> getWriter(final OutputStream output) {

		return new SerializationWriterSupport() {

			/* Native Avro writer. */
			DataFileWriter<T> writer;

			@Override
			protected Closeable doOpen() throws IOException {
				// Create reflective Avro schema by object class.
				Schema schema = ReflectData.get().getSchema(objectsClass);

				// Create Avro writer.
				writer = new DataFileWriter<T>(new ReflectDatumWriter<T>(schema));

				// Configure compression if specified.
				writer.setCodec(CompressionUtils.getAvroCompression(getCompressionAlias()));

				// Open a new file for data serialization using specified schema.
				writer.create(schema, output);

				// Return Avro writer for later release.
				return writer;
			}

			/**
			 * Writes objects using Avro serialization.
			 * 
			 * @see {@link DataFileWriter}
			 */
			@Override
			protected void doWrite(T object) throws IOException {
				writer.append(object);
			}
		};
	}

	/**
	 * Reads POJOs using <code>Avro</code> serialization.
	 */
	@Override
	public SerializationReader<T> getReader(String location) {

		return new SerializationReaderSupport(location) {

			DataFileStream<T> reader;

			@Override
			protected Closeable doOpen() throws IOException {

				Resource hdfsResource = getHdfsResourceLoader().getResource(location);

				reader = new DataFileStream<T>(hdfsResource.getInputStream(), new ReflectDatumReader<T>());

				return reader;
			}

			@Override
			protected T doRead() throws IOException {
				return reader.hasNext() ? reader.next() : null;
			}
		};
	}

	/**
	 * @return <b>.avro</b> is the default file extension for Avro serialization.
	 */
	@Override
	protected String getDefaultExtension() {
		return ".avro";
	}

}
