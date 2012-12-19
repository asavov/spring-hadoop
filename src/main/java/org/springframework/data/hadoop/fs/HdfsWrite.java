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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.serializer.JavaSerialization;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.util.ReflectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * Utility class providing 'write into HDFS' functionality. It's supposed to be used outside of Hadoop to move data into
 * HDFS. Features supported:
 * <ul>
 * <li>Write file: pass a source and HDFS destination and it writes the file into HDFS.</li>
 * <li>Write file with compression: pass a source, HDFS destination and a compression and it writes the file into HDFS
 * compressing its content on the fly.</li>
 * </ul>
 * 
 * @author Alex Savov
 */
// TODO: Update JavaDoc once we agree upon on the design.
public class HdfsWrite {

	static String SEQ_FILE_EXTENSION = ".seqfile";

	@Autowired
	private FileSystem fs;

	@Autowired
	private Configuration config;

	@Autowired
	private HdfsResourceLoader hdfsResourceLoader;

	private String codecAlias;

	/**
	 * @return the codecAlias
	 */
	public String getCodecAlias() {
		return codecAlias;
	}

	/**
	 * @param codecAlias Accepted values:
	 * <ul>
	 * <li>The short class name (without the package) of the compression codec that is specified within Hadoop
	 * configuration (under <i>io.compression.codecs</i> prop). If the short class name ends with 'Codec', then there
	 * are two aliases for the codec - the complete short class name and the short class name without the 'Codec'
	 * ending. For example for the 'GzipCodec' codec class name the alias are 'gzip' and 'gzipcodec' (case insensitive).
	 * If the codec is configured to be used by Hadoop this is the preferred way instead of passing the codec canonical
	 * name.</li>
	 * <li>The canonical class name of the compression codec that is specified within Hadoop configuration (under
	 * <i>io.compression.codecs</i> prop) or is present on the classpath.</li>
	 * </ul>
	 */
	public void setCodecAlias(String codecAlias) {
		this.codecAlias = codecAlias;
	}

	/**
	 * Writes the content of source resource to the destination.
	 * 
	 * @param source The source to read from.
	 * @param destination The destination to write to.
	 * 
	 * @throws IOException
	 */
	public void write(Resource source, String destination) throws IOException {

		CompressionCodec codec = getCodec();

		if (codec != null) {
			if (!destination.toLowerCase().endsWith(codec.getDefaultExtension().toLowerCase())) {
				destination += codec.getDefaultExtension();
			}
		}

		InputStream is = source.getInputStream();

		HdfsResource hdfsResource = (HdfsResource) hdfsResourceLoader.getResource(destination);

		// Open new HDFS file
		OutputStream os = hdfsResource.getOutputStream();

		// Apply compression
		if (codec != null) {
			os = codec.createOutputStream(os);
			// TODO: Eventually re-use underlying Compressor through CodecPool.
		}

		// Write source to HDFS destination
		IOUtils.copyBytes(is, os, config, /* close */true);
	}

	/**
	 * Writes Java object to a HDFS sequence file at provided destination.
	 * 
	 * The object is serialized based on its type by SequenceFiles pluggable serialization framework. Hadoop comes with
	 * four serializers: Avro, Java, Tether and Writable (the default serializer).
	 * 
	 * @param object
	 * @param destination
	 * @throws IOException
	 * 
	 * @see {@link Serialization}
	 * @see {@link SerializationFactory}
	 */
	public <T> void write(Collection<? extends T> objects, Class<T> objectsClass, String destination)
			throws IOException {

		initSerializations();

		if (!destination.toLowerCase().endsWith(SEQ_FILE_EXTENSION)) {
			destination += SEQ_FILE_EXTENSION;
		}

		HdfsResource hdfsResource = (HdfsResource) hdfsResourceLoader.getResource(destination);

		CompressionCodec codec = getCodec();
		
		SequenceFile.Writer writer;
		
		if (codec != null) {
			// TODO: @Costin
			// The question here is: what's THE key value and class when writing pojos?
			// Different serializations implies different class restrictions.
			// Since we delegate serialization to underlying Hadoop Serializers we are not aware what key class to pass.
			// About the value: we might autogenerate one or have a contract with the source object.
			// So far use NullWritable :)
			writer = SequenceFile.createWriter(fs, config, hdfsResource.getPath(), NullWritable.class,
					objectsClass, SequenceFile.CompressionType.BLOCK, codec);			
		} else {
			writer = SequenceFile.createWriter(fs, config, hdfsResource.getPath(), NullWritable.class,
							objectsClass, SequenceFile.CompressionType.NONE);			
		}

		try {
			for (T object : objects) {
				writer.append(NullWritable.get(), object);
			}
		} finally {
			writer.close();
		}
	}

	/**
	 * @return The codec to be used to compress the data on the fly while storing it onto HDFS if the
	 * <code>codecAlias</code> property is specified; <code>null</code> otherwise.
	 */
	private CompressionCodec getCodec() {

		if (!StringUtils.hasText(codecAlias)) {
			return null;
		}

		final CompressionCodecFactory codecFactory = new CompressionCodecFactory(config);

		// Find codec by canonical class name or by codec alias as specified in Hadoop
		// configuration
		CompressionCodec codec = codecFactory.getCodecByName(codecAlias);

		// If the codec is not configured within Hadoop try to load it from the classpath
		if (codec == null) {
			Class<?> codecClass = ClassUtils.resolveClassName(codecAlias, getClass().getClassLoader());

			// Instantiate codec and initialize it from configuration
			// org.apache.hadoop.util.ReflectionUtils design is specific to Hadoop env :)
			codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, config);
		}

		// TODO: Should we fall back to some default codec if not resolved?
		return codec;

	}

	/**
	 * Configure Hadoop to support WritableSerialization and JavaSerialization in addition to present ones.
	 * 
	 * NOTE: Only WritableSerialization is supported by default.
	 */
	private void initSerializations() {

		final String HADOOP_IO_SERIALIZATIONS = "io.serializations";

		String[] existing = (String[]) config.getStrings(HADOOP_IO_SERIALIZATIONS);

		String[] serializations = { WritableSerialization.class.getCanonicalName(),
				JavaSerialization.class.getCanonicalName() };

		String[] merged = StringUtils.mergeStringArrays(existing, serializations);

		config.set(HADOOP_IO_SERIALIZATIONS, StringUtils.arrayToCommaDelimitedString(merged));
	}

}
