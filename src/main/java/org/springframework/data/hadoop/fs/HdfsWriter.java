/*
 * Copyright 2012 the original author or authors.
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
package org.springframework.data.hadoop.fs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.ReflectionUtils;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.HadoopException;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * Utility class providing 'write to HDFS' functionality. It is supposed to be used outside of Hadoop to move data into
 * HDFS. Features supported:
 * <ul>
 * <li>Write resource: writes the content of {@link Resource} source to provided HDFS destination.</li>
 * <li>Write collection of Java objects: writes a collection of Java objects to provided HDFS destination. A
 * serialization format could be additionally specified to customize objects persistence.</li>
 * <li>Write with compression: same as above plus compress output data on the fly.</li>
 * </ul>
 * 
 * @author Alex Savov
 */
public class HdfsWriter {

	/**
	 * The interface represents a strategy to write a collection of Java objects to HDFS.
	 * 
	 * @see {@link SequenceFileFormat}
	 * @see {@link AvroSequenceFileFormat}
	 * @see {@link AvroFormat}
	 */
	// @Costin: Inner or separate interface? My take is inner one due to better scoping. More or less it is
	// designed/coupled to HdfsWriter. But I don't have strong preference :)
	public static interface SerializationFormat {

		// @Costin: should we "mirror" Hadoop SequenceFile serialization framework to some extend?
		// boolean accept(Class<?> objectsClass);

		/**
		 * Mirrors core {@link HdfsWriter#write(Collection, Class, String)} method encapsulating just a single
		 * serialization use-case, such as write to SequenceFiles using Java serialization.
		 * 
		 * @param objects The objects to write.
		 * @param objectsClass The class of objects to write.
		 * @param destination HDFS destination file path to write to.
		 * 
		 * @throws IOException
		 */
		<T> void write(Iterable<? extends T> objects, Class<T> objectsClass, String destination) throws IOException;

	}

	protected final Configuration configuration;
	protected final HdfsResourceLoader hdfsResourceLoader;

	/* The property is publicly configurable. Defaults to NO compression. */
	private String compressionAlias = null;

	/* The property is publicly configurable. Defaults to SequenceFileFormat. */
	private SerializationFormat serializationFormat = new SequenceFileFormat();

	/**
	 * Constructs a new <code>HdfsWriter</code> instance.
	 * 
	 * @param configuration A non-null Hadoop configuration to use.
	 * @param hdfsResourceLoader A non-null HDFS resource loader to use.
	 * 
	 * @throws IllegalArgumentException if some of the parameters is <code>null</code>
	 */
	public HdfsWriter(Configuration configuration, HdfsResourceLoader hdfsResourceLoader) {
		/*
		 * @Costin: what do you think about such a strict contract. The user is provided with enough tools
		 * (FactoryBeans, namespace handlers) to create & configure Configuration and FileSystem instances, so I'd
		 * rather not provide "yet another one mechanism" and I'll enforce him to give me ready-to-use objects.
		 */
		Assert.notNull(configuration, "A non-null Hadoop configuration is required.");
		Assert.notNull(hdfsResourceLoader, "A non-null HDFS resource loader is required.");

		this.configuration = configuration;
		this.hdfsResourceLoader = hdfsResourceLoader;
	}

	/**
	 * Gets the serialization format used to write a collection of Java objects to HDFS through
	 * {@link #write(Iterable, Class, String)} method.
	 * 
	 * @return the serialization The serialization format used to write a collection of Java objects to HDFS.
	 */
	public SerializationFormat getSerializationFormat() {
		return serializationFormat;
	}

	/**
	 * Sets the serialization format to be used to write a collection of Java objects to HDFS through
	 * {@link #write(Iterable, Class, String)} method.
	 * 
	 * @param serialization The serialization format to be used to write a collection of Java objects to HDFS.
	 */
	public void setSerializationFormat(SerializationFormat serialization) {
		this.serializationFormat = serialization;
	}

	/**
	 * @return the compressionAlias
	 */
	public String getCompressionAlias() {
		return compressionAlias;
	}

	/**
	 * @param compressionAlias
	 */
	public void setCompressionAlias(String compressionAlias) {
		this.compressionAlias = compressionAlias;
	}

	/**
	 * Writes the content of source resource to the destination.
	 * 
	 * @param source The source to read from.
	 * @param destination HDFS destination file path to write to.
	 */
	public void write(Resource source, String destination) {

		if (source == null) {
			// Silently return...
			return;
		}

		Assert.notNull(destination, "A non-null destination path is required.");

		CompressionCodec codec = getHadoopCodec(configuration, getCompressionAlias());

		if (codec != null) {
			if (!destination.toLowerCase().endsWith(codec.getDefaultExtension().toLowerCase())) {
				destination += codec.getDefaultExtension();
			}
		}

		InputStream inputStream = null;
		OutputStream outputStream = null;
		try {
			inputStream = source.getInputStream();

			HdfsResource hdfsResource = (HdfsResource) hdfsResourceLoader.getResource(destination);

			// Open new HDFS file
			outputStream = hdfsResource.getOutputStream();

			// Apply compression
			if (codec != null) {
				outputStream = codec.createOutputStream(outputStream);
				// TODO: Eventually re-use underlying Compressor through CodecPool.
			}

			// Write source to HDFS destination
			IOUtils.copyBytes(inputStream, outputStream, configuration, /* close */false);

		} catch (IOException ioExc) {

			throw new HadoopException("Cannot write resource: " + ioExc.getMessage(), ioExc);

		} finally {
			IOUtils.closeStream(outputStream);
			IOUtils.closeStream(inputStream);
		}
	}

	/**
	 * Writes a collection of Java object to HDFS at provided destination.
	 * 
	 * Objects serialization is delegated to configured {@link SerializationFormat}.
	 * 
	 * @param objects The objects to write.
	 * @param objectsClass The class of objects to write.
	 * @param destination HDFS destination file path to write to.
	 */
	public <T> void write(Iterable<? extends T> objects, Class<T> objectsClass, String destination) {

		if (objects == null) {
			// Silently return...
			return;
		}

		Assert.notNull(objectsClass, "A non-null object class is required.");
		Assert.notNull(destination, "A non-null destination path is required.");

		SerializationFormat sFormat = getSerializationFormat();

		Assert.notNull(sFormat, "A non-null serialization format is required to write a collection of objects to HDFS.");

		if (sFormat instanceof SerializationFormatSupport) {
			SerializationFormatSupport serializationFormatSupport = (SerializationFormatSupport) sFormat;

			// Propagate HdfsWriter properties to underlying SerializationFormat
			serializationFormatSupport.setConfiguration(configuration);
			serializationFormatSupport.setHdfsResourceLoader(hdfsResourceLoader);
			serializationFormatSupport.setCompressionAlias(getCompressionAlias());
		}

		try {
			// Delegate to core 'write objects' logic
			sFormat.write(objects, objectsClass, destination);

		} catch (IOException ioExc) {
			throw new HadoopException("Cannot write objects: " + ioExc.getMessage(), ioExc);
		}
	}

	/**
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
	 */
	protected static CompressionCodec getHadoopCodec(Configuration conf, String compressionAlias) {

		if (!StringUtils.hasText(compressionAlias)) {
			return null;
		}

		final CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);

		// Find codec by canonical class name or by codec alias as specified in Hadoop configuration
		// @Costin: TODO: The method is NOT available in hadoop-core.1.0.4.jar, but in CDH3 version. How to handle it?
		CompressionCodec compression = codecFactory.getCodecByName(compressionAlias);

		// If the codec is not configured within Hadoop try to load it from the classpath
		if (compression == null) {
			Class<?> compressionClass = ClassUtils
					.resolveClassName(compressionAlias, HdfsWriter.class.getClassLoader());

			// Instantiate codec and initialize it from configuration
			// org.apache.hadoop.util.ReflectionUtils design is specific to Hadoop env :)
			compression = (CompressionCodec) ReflectionUtils.newInstance(compressionClass, conf);
		}

		// TODO: Should we fall back to some default codec if not resolved?
		return compression;
	}

}
