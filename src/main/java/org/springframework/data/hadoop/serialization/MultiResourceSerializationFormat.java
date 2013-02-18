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
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.IOUtils;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;

/**
 * An implementation of {@link SerializationFormat} which serializes multiple Spring {@link Resource}s to a single
 * <code>Avro</code> container file in HDFS. This is useful if we want to store a large number of files in HDFS, and do
 * so without hitting the NameNode memory limits.
 * 
 * <p>
 * The Avro container file used internally consists of zero or more records, where each record consists of two fields
 * containing the resource key and the resource content in byte form.
 * 
 * @author Alex Savov
 */
// TODO: Decide which class to go with: MultiResourceSerializationFormat or MultiResourceSerializationFormat2
// This impl is based on native Avro API and its GenericRecord and GenericDatumWriter
// It supports/stores resource key (configurable through SerializationKeyProvider) and resource content (as byte array)
public class MultiResourceSerializationFormat extends SerializationFormatSupport<Resource> {

	protected static final String RESOURCE_KEY_FIELD = "resourceKey";
	protected static final String RESOURCE_CONTENT_FIELD = "resourceContent";

	// TODO: this is not extensible. we need to generate underlying Avro schemaon the fly.
	// Maybe this is a "pro" for MultiResourceSerializationFormat2.
	protected static final String SCHEMA_JSON = "{\"type\": \"record\", \"name\": \""
			+ MultiResourceSerializationFormat.class.getSimpleName() + "\", " + "\"fields\": [" + "{\"name\":\""
			+ RESOURCE_KEY_FIELD + "\", \"type\":\"string\"}," + "{\"name\":\"" + RESOURCE_CONTENT_FIELD
			+ "\", \"type\":\"bytes\"}]}";

	protected static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_JSON);

	/* This property is publicly configurable. */
	private SerializationKeyProvider resourceKeyProvider = new ReflectiveSerializationKeyProvider(Resource.class,
			"description");

	/**
	 * Writes Spring {@link Resource}s to an <code>Avro</code> container file.
	 */
	protected SerializationWriterSupport createWriter(final OutputStream output) {

		return new SerializationWriterSupport() {

			/* Native Avro writer. */
			DataFileWriter<GenericRecord> writer;

			@Override
			protected Closeable doOpen() throws IOException {

				writer = new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>());

				// Configure compression if specified.
				writer.setCodec(CompressionUtils.getAvroCompression(getCompressionAlias()));

				// Open a new file for data serialization using specified SCHEMA.
				writer.create(SCHEMA, output);

				// Return Avro writer for later release.
				return writer;
			}

			@Override
			protected void doWrite(Resource resource) throws IOException {
				GenericRecord record = new GenericData.Record(SCHEMA);

				record.put(RESOURCE_KEY_FIELD, getResourceKeyProvider().getKey(resource).toString());
				record.put(RESOURCE_CONTENT_FIELD, ByteBuffer.wrap(IOUtils.toByteArray(resource.getInputStream())));

				writer.append(record);
			}
		};
	}

	/**
	 * Reads Spring {@link Resource}s from an <code>Avro</code> container file.
	 */
	@Override
	protected SerializationReaderSupport createReader(final String location) {

		return new SerializationReaderSupport() {

			/* Native Avro reader. */
			DataFileStream<GenericRecord> reader;

			@Override
			protected Closeable doOpen() throws IOException {

				final Resource hdfsResource = getHdfsResourceLoader().getResource(location);
				final InputStream inputStream = hdfsResource.getInputStream();

				reader = new DataFileStream<GenericRecord>(inputStream, new GenericDatumReader<GenericRecord>());

				return reader;
			}

			@Override
			protected Resource doRead() throws IOException {
				if (!reader.hasNext()) {
					return null;
				}

				final GenericRecord genericRecord = reader.next();

				String description = ((Utf8) genericRecord.get(RESOURCE_KEY_FIELD)).toString();

				byte[] content = ((ByteBuffer) genericRecord.get(RESOURCE_CONTENT_FIELD)).array();

				return new ByteArrayResource(content, description);
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

	/**
	 * The instance provides the key of each {@link Resource} stored. The value returned is stored as string according
	 * to its {@link #toString()} method. An exemplary implementation might return {@link Resource#getDescription()} or
	 * {@link Resource#getFilename()}.
	 * 
	 * @param resourceKeyProvider
	 */
	public void setResourceKeyProvider(SerializationKeyProvider resourceKeyProvider) {
		this.resourceKeyProvider = resourceKeyProvider;
	}

	protected SerializationKeyProvider getResourceKeyProvider() {
		return resourceKeyProvider;
	}

}
