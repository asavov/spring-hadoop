/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.hadoop.serialization;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.ReflectionUtils;
import org.springframework.data.hadoop.HadoopException;
import org.springframework.data.hadoop.fs.HdfsResource;
import org.springframework.data.hadoop.fs.HdfsResourceLoader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * Utility class providing 'write to HDFS' functionality. It leverages serialization formats to do the actual objects
 * serialization and thus serve as a bridge to Hadoop HDFS.
 * 
 * @author Alex Savov
 */
public class HdfsWriter {

	protected final Configuration configuration;
	protected final HdfsResourceLoader hdfsResourceLoader;

	/* The property is publicly configurable. */
	private SerializationFormat<?> serializationFormat;

	/**
	 * Constructs a new <code>HdfsWriter</code> instance.
	 * 
	 * @param configuration A non-null Hadoop configuration to use.
	 * @param hdfsResourceLoader A non-null HDFS resource loader to use.
	 * 
	 * @throws IllegalArgumentException if some of the parameters is <code>null</code>
	 */
	public HdfsWriter(Configuration configuration, HdfsResourceLoader hdfsResourceLoader) {

		Assert.notNull(configuration, "A non-null Hadoop configuration is required.");
		Assert.notNull(hdfsResourceLoader, "A non-null HDFS resource loader is required.");

		this.configuration = configuration;
		this.hdfsResourceLoader = hdfsResourceLoader;
	}

	/**
	 * Gets the serialization format used to write objects to HDFS through {@link #write(Object, String)} method.
	 * 
	 * @return the serialization The serialization format used to write objects to HDFS.
	 */
	public SerializationFormat<?> getSerializationFormat() {
		return serializationFormat;
	}

	/**
	 * Sets the serialization format to be used to write objects to HDFS through {@link #write(Object, String)} method.
	 * 
	 * @param serialization The serialization format to be used to write objects to HDFS.
	 */
	public void setSerializationFormat(SerializationFormat<?> serialization) {
		this.serializationFormat = serialization;
	}

	/**
	 * @param source
	 * @param destination
	 */
	public <T> void write(T source, String destination) {
		if (source == null) {
			// Silently return...
			return;
		}

		Assert.notNull(destination, "A non-null destination path is required.");

		SerializationFormat<T> serializationFormat = (SerializationFormat<T>) getSerializationFormat();

		Assert.notNull(serializationFormat, "A non-null serialization format is required.");

		OutputStream outputStream = null;
		try {
			// Append SerializationFormat extension to the destination (if not present)
			String extension = serializationFormat.getExtension();
			if (StringUtils.hasText(extension)) {
				if (!destination.toLowerCase().endsWith(extension.toLowerCase())) {
					destination += extension;
				}
			}

			// Resolve destination to HDFS Resource.
			HdfsResource hdfsDestinationResource = (HdfsResource) hdfsResourceLoader.getResource(destination);

			// Open destination resource for writing.
			outputStream = hdfsDestinationResource.getOutputStream();

			// Delegate to core SerializationFormat logic.
			serializationFormat.serialize(source, outputStream);

		} catch (IOException ioExc) {
			throw new HadoopException("Cannot write the source object to HDFS: " + ioExc.getMessage(), ioExc);
		} finally {
			IOUtils.closeStream(outputStream);
		}
	}

	/**
	 * @param conf Hadoop configuration to use.
	 * @param compressionAlias <ul>
	 * <li>The short class name (without the package) of the compression codec that is specified within Hadoop
	 * configuration (under <i>io.compression.codecs</i> prop). If the short class name ends with 'Codec', then there
	 * are two aliases for the codec - the complete short class name and the short class name without the 'Codec'
	 * ending. For example for the 'GzipCodec' codec class name the alias are 'gzip' and 'gzipcodec' (case insensitive).
	 * If the codec is configured to be used by Hadoop this is the preferred way instead of passing the codec canonical
	 * name.</li>
	 * <li>The canonical class name of the compression codec that is specified within Hadoop configuration (under
	 * <i>io.compression.codecs</i> prop) or is present on the classpath.</li>
	 * </ul>
	 * 
	 * @return The codec to be used to compress the data on the fly while storing it onto HDFS, if the
	 * <code>compressionAlias</code> property is specified; <code>null</code> otherwise.
	 * 
	 * @throws IllegalArgumentException if the codec class name is not resolvable
	 * @throws RuntimeException if the codec class is not instantiable
	 */
	protected static CompressionCodec getHadoopCodec(Configuration conf, String compressionAlias) {

		if (!StringUtils.hasText(compressionAlias)) {
			return null;
		}

		final CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);

		// Find codec by canonical class name or by codec alias as specified in Hadoop configuration
		CompressionCodec compression = codecFactory.getCodecByName(compressionAlias);

		// If the codec is not configured within Hadoop try to load it from the classpath
		if (compression == null) {
			Class<?> compressionClass = ClassUtils
					.resolveClassName(compressionAlias, HdfsWriter.class.getClassLoader());

			// Instantiate codec and initialize it from configuration
			// org.apache.hadoop.util.ReflectionUtils design is specific to Hadoop env :)
			compression = (CompressionCodec) ReflectionUtils.newInstance(compressionClass, conf);
		}

		return compression;
	}

}
