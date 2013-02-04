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
// TODO: Maybe add support for write/read of MULTIPLE small files using Avro/SeqFile format.
// TODO: The contract of the class MUST be cleared! @Costin: any ideas?
public class ResourceSerializationFormat extends SerializationFormatSupport<Resource> implements InitializingBean {

	/* This property is publicly configurable. */
	private Configuration configuration;

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

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(getConfiguration(), "A non-null Hadoop configuration is required.");
	}

	/**
	 * Writes the content of Spring {@link Resource}s to a single HDFS location.
	 */
	@Override
	public SerializationWriter<Resource> getWriter(final OutputStream output) {
		// Extend and customize Serialization Writer template
		return new SerializationWriterSupport() {

			private OutputStream outputStream = output;

			@Override
			protected Closeable doOpen() throws IOException {
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
						IOUtils.closeStream(outputStream);
						CodecPool.returnCompressor(compressor);
					}
				};
			}

			@Override
			protected void doWrite(Resource source) throws IOException {
				InputStream inputStream = null;
				try {
					inputStream = source.getInputStream();

					// Write source to HDFS destination
					copyBytes(inputStream, outputStream, getConfiguration(), /* close */false);
				} finally {
					closeStream(inputStream);
				}
			}
		};
	}

	/**
	 * Reads the content of the HDFS resource at specified location.
	 */
	@Override
	public SerializationReader<Resource> getReader(final String location) {
		// Extend and customize Serialization Reader template
		return new SerializationReaderSupport() {

			private boolean isRead = false;

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
	 * specified. <code>empty</code> string otherwise.
	 */
	@Override
	protected String getDefaultExtension() {
		CompressionCodec codec = CompressionUtils.getHadoopCompression(getConfiguration(), getCompressionAlias());

		return codec != null ? codec.getDefaultExtension() : "";
	}

}
