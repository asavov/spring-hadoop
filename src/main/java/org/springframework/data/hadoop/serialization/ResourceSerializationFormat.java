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
import static org.apache.hadoop.io.IOUtils.copyBytes;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;

/**
 * An implementation of {@link SerializationFormat} which serializes Spring {@link Resource}s to HDFS.
 * 
 * @author Alex Savov
 */
public class ResourceSerializationFormat extends SerializationFormatSupport<Resource> implements InitializingBean {

	private static final String DEFAULT_LINE_SEPARATOR = System.getProperty("line.separator");

	/* This property is publicly configurable. */
	private Configuration configuration;

	/* This property is publicly configurable. */
	private String resourceSeparator = DEFAULT_LINE_SEPARATOR;

	/**
	 * Sets the Hadoop configuration for this <code>SerializationFormat</code>.
	 * 
	 * @param configuration The configuration to use.
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	protected Configuration getConfiguration() {
		return configuration;
	}

	/**
	 * Sets the resource separator to use while writing. <code>null</code> means no separator between resources.
	 * Defaults to the System property <code>line.separator</code>.
	 * 
	 * @param resourceSeparator the resource separator to set
	 */
	public void setResourceSeparator(String resourceSeparator) {
		this.resourceSeparator = resourceSeparator;
	}

	protected String getResourceSeparator() {
		return resourceSeparator;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(getConfiguration(), "A non-null Hadoop configuration is required.");
	}

	/**
	 * Writes the content of Spring {@link Resource}s to a single HDFS location.
	 */
	@Override
	protected SerializationWriterSupport createWriter(final OutputStream output) {
		// Extend and customize Serialization Writer template
		return new SerializationWriterSupport() {

			private OutputStream outputStream = output;

			private InputStream resourceSeparatorInputStream;

			@Override
			protected Closeable doOpen() throws IOException {

				resourceSeparatorInputStream = null;

				CompressionCodec codec = CompressionUtils.getHadoopCompression(getConfiguration(),
						getCompressionAlias());

				// If a compression is not specified and if passed stream does have compression capabilities...
				if (codec == null || CompressionOutputStream.class.isInstance(outputStream)) {
					// ...just return original stream untouched
					return outputStream;
				}

				// Eventually re-use Compressor from underlying CodecPool
				final Compressor compressor = CodecPool.getCompressor(codec);

				// Create compression stream wrapping passed stream
				outputStream = codec.createOutputStream(outputStream, compressor);

				return new Closeable() {

					@Override
					public void close() throws IOException {
						resourceSeparatorInputStream = null;
						IOUtils.closeStream(outputStream);
						CodecPool.returnCompressor(compressor);
					}
				};
			}

			@Override
			protected void doWrite(Resource source) throws IOException {
				InputStream inputStream = null;
				try {
					writeSeparator();

					inputStream = source.getInputStream();

					// Write source to HDFS destination
					copyBytes(inputStream, outputStream, getConfiguration(), /* close */false);

				} finally {
					closeStream(inputStream);
				}
			}

			protected void writeSeparator() throws IOException {
				if (getResourceSeparator() == null) {
					return;
				}

				if (resourceSeparatorInputStream == null) {

					// First call inits 'resourceSeparatorInputStream' and does not write anything

					resourceSeparatorInputStream = new ByteArrayInputStream(getResourceSeparator().getBytes("UTF-8"));

					return;
				}

				resourceSeparatorInputStream.reset();

				// Write resource separator to HDFS destination
				copyBytes(resourceSeparatorInputStream, outputStream, getConfiguration(), /* close */false);
			}
		};
	}

	/**
	 * Reads the content of the HDFS resource at specified location.
	 */
	@Override
	protected SerializationReaderSupport createReader(final String location) {
		// Extend and customize Serialization Reader template
		return new SerializationReaderSupport() {

			private boolean isRead;

			@Override
			protected Closeable doOpen() throws IOException {
				isRead = false;
				return null;
			}

			@Override
			protected Resource doRead() throws IOException {
				if (isRead) {
					return null;
				}

				try {
					return getHdfsResourceLoader().getResource(location);
				} finally {
					isRead = true;
				}
			}
		};
	}

	/**
	 * @return compression default {@link CompressionCodec#getDefaultExtension() extension} if compression alias is
	 * specified; <code>empty</code> string otherwise.
	 */
	@Override
	protected String getDefaultExtension() {
		CompressionCodec codec = CompressionUtils.getHadoopCompression(getConfiguration(), getCompressionAlias());

		return codec != null ? codec.getDefaultExtension() : "";
	}

}
