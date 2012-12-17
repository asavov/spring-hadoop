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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.ReflectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Utility class providing 'write into HDFS' functionality. It's supposed to be used
 * outside of Hadoop to move data into HDFS. Features supported:
 * <ul>
 * <li>Write file: pass a source and HDFS destination and it writes the file into HDFS.</li>
 * <li>Write file with compression: pass a source, HDFS destination and a compression and
 * it writes the file into HDFS compressing its content on the fly.</li>
 * </ul>
 *
 * @author Alex Savov
 */
// TODO: Update JavaDoc once we agree upon on the design.
public class HdfsWrite {

	@Autowired
	private FileSystem fs;

	@Autowired
	private Configuration config;

	@Autowired
	private HdfsResourceLoader hdfsResourceLoader;

	/**
	 * Writes the content of source resource to the destination.    
	 * 
	 * @param source The source to read from.
	 * @param destination The destination to write to.
	 *
	 * @throws IOException
	 */
	public void write(Resource source, String destination) throws IOException {

		final InputStream is = source.getInputStream();

		final HdfsResource hdfsResource = (HdfsResource) hdfsResourceLoader.getResource(destination);

		// Open new HDFS file
		final OutputStream os = hdfsResource.getOutputStream();

		// Write source to HDFS destination
		IOUtils.copyBytes(is, os, config, /* close */true);
	}

	/**
	 * Writes the content of source resource to the destination applying compression on the fly.
	 * 
	 * @param source The source to read from.
	 * @param destination The destination to write to.
	 * @param codecAlias Accepted values:
	 *            <ul>
	 *            <li>The short class name (without the package) of the compression codec
	 *            that is specified within Hadoop configuration (under
	 *            <i>io.compression.codecs</i> prop). If the short class name ends with
	 *            'Codec', then there are two aliases for the codec - the complete short
	 *            class name and the short class name without the 'Codec' ending. For
	 *            example for the 'GzipCodec' codec class name the alias are 'gzip' and
	 *            'gzipcodec' (case insensitive). If the codec is configured to be used
	 *            by Hadoop this is the preferred way instead of passing the codec
	 *            canonical name.</li>
	 *            <li>The canonical class name of the compression codec that is specified
	 *            within Hadoop configuration (under <i>io.compression.codecs</i> prop)
	 *            or is present on the classpath.</li>
	 *            </ul>
	 *
	 * @throws IOException
	 */
	public void write(Resource source, String destination, String codecAlias) throws IOException {

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

			/*
			 * NOTE: One of the overheads to using compression codecs is that they can be
			 * expensive to create. Using the Hadoop ReflectionUtils class will result in
			 * some of the reflection overhead associated with creating the instance
			 * being cached in ReflectionUtils, which should speed up subsequent creation
			 * of the codec. A better option would be to use the CompressionCodecFactory,
			 * which provides caching of the codecs themselves.
			 */
		}

		// TODO: Should we fall back to some default codec if not resolved?

		// Once CompressionCodec is resolved delegate to core method.
		write(source, destination, codec);
	}

	/**
	 * Writes the content of source resource to the destination applying compression on the fly.
	 * 
	 * @param source The source to read from.
	 * @param destination The destination to write to.
	 * @param codec The codec to be used to compress the data on the fly while storing it
	 *            onto HDFS.
	 *
	 * @throws IOException
	 */
	public void write(Resource source, String destination, CompressionCodec codec) throws IOException {

		// TODO: Should we fall back to default codec (such as Snappy) if not passed?
		Assert.notNull(codec);

		final InputStream is = source.getInputStream();

		if (!destination.toLowerCase().endsWith(codec.getDefaultExtension().toLowerCase())) {
			destination += codec.getDefaultExtension();
		}

		final HdfsResource hdfsResource = (HdfsResource) hdfsResourceLoader.getResource(destination);

		// Open new HDFS file
		OutputStream os = hdfsResource.getOutputStream();

		// Apply compression
		os = codec.createOutputStream(os);
		// TODO: Eventually re-use underlying Compressor through CodecPool.

		// Write source to HDFS destination
		IOUtils.copyBytes(is, os, config, /* close */true);
	}

}
