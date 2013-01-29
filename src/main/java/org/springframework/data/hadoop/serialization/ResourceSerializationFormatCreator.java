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
import java.io.FilterOutputStream;
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
import org.springframework.data.hadoop.HadoopException;
import org.springframework.util.Assert;

/**
 * An implementation of {@link SerializationFormatCreator} which serializes Spring {@link Resource}s to HDFS.
 * 
 * @author Alex Savov
 */
public class ResourceSerializationFormatCreator extends SerializationFormatCreatorSupport<Resource> implements
		InitializingBean {

	/* This property is publicly configurable. */
	private Configuration configuration;

	/**
	 * Sets the Hadoop configuration for this <code>SerializationFormatCreator</code>.
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

	@Override
	public SerializationFormat<Resource> createSerializationFormat(final OutputStream output) {
		// Extend and customize SerializationFormat template
		return new SerializationFormatSupport() {

			private OutputStream compressedOutput;

			@Override
			protected Closeable doOpen() throws IOException {
				// Apply compression if specified
				return compressedOutput = compressStream(output);
			}

			@Override
			protected void doSerialize(Resource source) throws IOException {
				InputStream inputStream = null;
				try {
					inputStream = source.getInputStream();

					// Write source to HDFS destination
					IOUtils.copyBytes(inputStream, compressedOutput, getConfiguration(), /* close */false);

				} catch (IOException ioExc) {

					throw new HadoopException("Cannot write resource: " + ioExc.getMessage(), ioExc);

				} finally {
					IOUtils.closeStream(inputStream);
				}
			}
		};
	}

	/**
	 * @return compression default {@link CompressionCodec#getDefaultExtension() extension} if compression alias is
	 * specified. <code>empty</code> string otherwise.
	 */
	@Override
	public String getExtension() {
		CompressionCodec codec = CompressionUtils.getHadoopCompression(getConfiguration(), getCompressionAlias());

		return codec != null ? codec.getDefaultExtension() : "";
	}

	/**
	 * @param outputStream The original output stream.
	 * @return original output stream if compression alias is not set or if original stream is an instance of
	 * <code>CompressionOutputStream</code>. Otherwise return <code>CompressionOutputStream</code> wrapping original
	 * output stream.
	 */
	private OutputStream compressStream(OutputStream outputStream) {

		CompressionCodec codec = CompressionUtils.getHadoopCompression(getConfiguration(), getCompressionAlias());

		// If a codec not is specified and if passed stream does have compression capabilities...
		if (codec == null || CompressionOutputStream.class.isInstance(outputStream)) {
			// just return original stream untouched
			return outputStream;
		}

		try {
			// Eventually re-use Compressor from underlying CodecPool
			final Compressor compressor = CodecPool.getCompressor(codec);

			// Create compression stream wrapping passed stream
			CompressionOutputStream compressionStream = codec.createOutputStream(outputStream, compressor);

			// Decorate the compression stream to release the Compressor upon close
			return new FilterOutputStream(compressionStream) {
				@Override
				public void close() throws IOException {
					try {
						super.close();
					} finally {
						CodecPool.returnCompressor(compressor);
					}
				}
			};
		} catch (IOException ioExc) {
			throw new HadoopException("Cannot open compressed output stream.", ioExc);
		}
	}

}
