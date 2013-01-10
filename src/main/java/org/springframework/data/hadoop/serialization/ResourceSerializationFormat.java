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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.HadoopException;

/**
 * @author Alex Savov
 */
public class ResourceSerializationFormat extends CompressedSerializationFormat<Resource> {

	private Configuration configuration;

	@Override
	public void serialize(Resource source, OutputStream outputStream) throws IOException {

		CompressionFormat compressionFormat = new CompressionFormat(getConfiguration(), getCompressionAlias());

		InputStream inputStream = null;
		try {
			inputStream = source.getInputStream();

			outputStream = compressionFormat.convert(outputStream);

			// Write source to HDFS destination
			IOUtils.copyBytes(inputStream, outputStream, getConfiguration(), /* close */false);

		} catch (IOException ioExc) {

			throw new HadoopException("Cannot write resource: " + ioExc.getMessage(), ioExc);

		} finally {
			IOUtils.closeStream(compressionFormat);
			IOUtils.closeStream(inputStream);
		}
	}

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
	public String getExtension() {

		CompressionCodec codec = CompressionUtils.getHadoopCompression(getConfiguration(), getCompressionAlias());

		return codec != null ? codec.getDefaultExtension() : "";
	}

	// TODO: An experimental class modeling compression format and
	// encapsulating compression logic used by Serialization formats.
	// It will be good to apply it on other Serialization formats.
	// Unfortunately other SerFormats require re-configuration of underlying objects,
	// such as avro.DataFileWriter and SequenceFile.Writer.
	// In other words changing/wrapping of OutputStream is not enough.
	// @Costin: Any thoughts :)
	private static class CompressionFormat implements Converter<OutputStream, OutputStream>, Closeable {

		private final CompressionCodec codec;

		private Compressor compressor;

		public CompressionFormat(Configuration configuration, String compressionAlias) {
			codec = CompressionUtils.getHadoopCompression(configuration, compressionAlias);
		}

		@Override
		public void close() {
			CodecPool.returnCompressor(compressor);
		}

		@Override
		public OutputStream convert(OutputStream outputStream) {

			// If a codec is specified and if passed stream does not have compression capabilities...
			if (codec != null && !CompressionOutputStream.class.isInstance(outputStream)) {

				// Eventually re-use Compressor from underlying CodecPool
				compressor = CodecPool.getCompressor(codec);

				try {
					// Create compression stream wrapping passed stream
					outputStream = codec.createOutputStream(outputStream, compressor);
				} catch (IOException ioExc) {
					throw new HadoopException("Cannot open compressed output stream.", ioExc);
				}
			}

			return outputStream;
		}
	}

}
