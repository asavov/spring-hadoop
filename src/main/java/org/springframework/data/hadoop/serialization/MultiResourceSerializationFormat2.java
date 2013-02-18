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
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;

/**
 * An implementation of {@link SerializationFormat} which serializes multiple Spring {@link Resource}s to a single
 * <code>Avro</code> container file in HDFS. This is useful if we want to store a large number of files in HDFS, and do
 * so without hitting the NameNode memory limits.
 * 
 * @author Alex Savov
 */
// TODO: Decide which class to go with: MultiResourceSerializationFormat or MultiResourceSerializationFormat2
// This impl is based on Avro Serialization Format API and its reflection capabilities
// It supports/store arbitrary set of props (configurable and extensible) and resource content (as byte array)
public class MultiResourceSerializationFormat2 extends SerializationFormatSupport<Resource> implements InitializingBean {

	/**
	 * The POJO is the counterpart of <code>Resource</code> class that is suitable to be written by Avro REFLECTIVE
	 * writer. It models the <code>Resource</code> as a content (in byte array form) and a map of string proporties
	 * (such as description, url, filename, etc.). This is the class passed to Avro format and used as a source for
	 * reflective Avro schema generation. It must meet Avro's requirements for types accepted/supported.
	 */
	public static class AvroResource {

		/**
		 * Models {@link Resource#getInputStream()} as an array of bytes.
		 */
		public byte[] content;

		/**
		 * Models all other <code>Resource</code> properties as a map of string properties.
		 */
		// We are a little bit restrictive but that's enforced by Avro reflective support.
		public Map<String, String> props = new HashMap<String, String>();

	}

	/**
	 * Converts between <code>Resource</code> object and a corresponding POJO that is suitable to be written by Avro
	 * reflective writer.
	 */
	public static interface ResourceToAvroResourceConverter {

		public abstract AvroResource toAvroResource(Resource resource);

		public abstract Resource toResource(AvroResource avroResource);
	}

	// Avro format delegate used to write AvroResource. We rely on its (respectively Avro's) reflection support.
	protected AvroFormat<AvroResource> avroFormatDelegate;

	// The Resource-to-Avro converter to use.
	protected ResourceToAvroResourceConverter resourceConverter = new ContentAndDescriptionResourceConverter();

	/**
	 * Writes Spring {@link Resource}s to an <code>Avro</code> container file.
	 */
	protected SerializationWriterSupport createWriter(final OutputStream output) {

		return new SerializationWriterSupport() {

			/* Avro format writer. */
			SerializationWriter<AvroResource> writer;

			@Override
			protected Closeable doOpen() throws IOException {
				// Return Avro format writer for later release.
				return writer = avroFormatDelegate.getWriter(output);
			}

			@Override
			protected void doWrite(Resource resource) throws IOException {
				writer.write(resourceConverter.toAvroResource(resource));
			}
		};
	}

	/**
	 * Reads Spring {@link Resource}s from an <code>Avro</code> container file.
	 */
	@Override
	protected SerializationReaderSupport createReader(final String location) {

		return new SerializationReaderSupport() {

			/* Avro format reader. */
			SerializationReader<AvroResource> reader;

			@Override
			protected Closeable doOpen() throws IOException {
				// Return Avro reader for later release.
				return reader = avroFormatDelegate.getReader(location);
			}

			@Override
			protected Resource doRead() throws IOException {
				AvroResource avroResource = reader.read();

				return (avroResource != null) ? resourceConverter.toResource(avroResource) : null;
			}
		};
	}

	/**
	 * @return <b>.avro</b> is the default file extension for Avro serialization.
	 */
	@Override
	protected String getDefaultExtension() {
		return avroFormatDelegate.getDefaultExtension();
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		avroFormatDelegate = new AvroFormat<AvroResource>(AvroResource.class);
		avroFormatDelegate.setCompressionAlias(getCompressionAlias());
		avroFormatDelegate.setHdfsResourceLoader(getHdfsResourceLoader());
		avroFormatDelegate.setLazyOpenReader(lazyOpenReader);
		avroFormatDelegate.setLazyOpenWriter(lazyOpenWriter);
		avroFormatDelegate.setExtension(getExtension());
	}

	/**
	 * Default implementation that models <code>Resource</code> as a content-description pair and returns
	 * <code>ByteArrayResource</code> instances.
	 */
	public static class ContentAndDescriptionResourceConverter implements ResourceToAvroResourceConverter {

		public static final String DESCRIPTION = "description";

		/**
		 * Models <code>Resource</code> as a content-description pair.
		 */
		public AvroResource toAvroResource(Resource resource) {

			AvroResource avroResource = new AvroResource();

			InputStream inputStream = null;
			try {
				avroResource.content = IOUtils.toByteArray(inputStream = resource.getInputStream());
			} catch (IOException ioExc) {
				throw new RuntimeException("Unable to read content of " + resource, ioExc);
			} finally {
				IOUtils.closeQuietly(inputStream);
			}

			avroResource.props.put(DESCRIPTION, resource.getDescription());

			return avroResource;
		}

		/**
		 * Creates <code>ByteArrayResource</code> from stored content-description pair.
		 */
		public Resource toResource(AvroResource avroResource) {
			return new ByteArrayResource(avroResource.content, avroResource.props.get(DESCRIPTION));
		}
	}

}
