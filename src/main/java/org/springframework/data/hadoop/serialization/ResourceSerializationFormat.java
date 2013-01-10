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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.HadoopException;

/**
 * 
 * @author asavov
 */
public class ResourceSerializationFormat extends CompressedSerializationFormat<Resource> {

	private Configuration configuration;

	@Override
	public void serialize(Resource source, OutputStream outputStream) throws IOException {
		if (source == null) {
			// Silently return...
			return;
		}

		InputStream inputStream = null;
		try {
			inputStream = source.getInputStream();

			CompressionCodec codec = CompressionUtils.getHadoopCompression(getConfiguration(), getCompressionAlias());

			// Apply compression
			if (codec != null) {
				outputStream = codec.createOutputStream(outputStream);
				// TODO: Eventually re-use underlying Compressor through CodecPool.
			}

			// Write source to HDFS destination
			IOUtils.copyBytes(inputStream, outputStream, getConfiguration(), /* close */false);

		} catch (IOException ioExc) {

			throw new HadoopException("Cannot write resource: " + ioExc.getMessage(), ioExc);

		} finally {
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

	public Configuration getConfiguration() {
		return configuration;
	}

	@Override
	public String getExtension() {

		CompressionCodec codec = CompressionUtils.getHadoopCompression(getConfiguration(), getCompressionAlias());

		return codec != null ? codec.getDefaultExtension() : "";
	}

}
