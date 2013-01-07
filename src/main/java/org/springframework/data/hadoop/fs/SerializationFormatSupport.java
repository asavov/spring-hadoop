/*
 * Copyright 2004-2012 the original author or authors.
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
package org.springframework.data.hadoop.fs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.fs.HdfsWriter.SerializationFormat;

/**
 * The class provides support commonly needed by {@link SerializationFormat} implementations. This includes:
 * <ul>
 * <li>Resolving of passed <code>destination</code> to HDFS {@link Resource}.</li>
 * <li>Exposing hook methods to allow 'write of objects' customization.</li>
 * </ul>
 * 
 * @author Alex Savov
 */
public abstract class SerializationFormatSupport implements SerializationFormat {

	private Configuration configuration;

	private HdfsResourceLoader hdfsResourceLoader;

	private String compressionAlias;

	/**
	 * Sets the Hadoop configuration for this <code>SerializationFormat</code>.
	 * 
	 * <p>
	 * The method is called by {@link HdfsWriter#write(Iterable, Class, String) HdfsWriter} prior a call to this
	 * serialization format {@link #write(Iterable, Class, String) write} method.
	 * 
	 * @param configuration The configuration to use.
	 */
	protected void setConfiguration(Configuration configuration) {
		// TODO: Should we clone passed Configuration or should we use it as it is?
		this.configuration = configuration;
	}

	protected Configuration getConfiguration() {
		return configuration;
	}

	/**
	 * Sets the HDFS resource loader for this <code>SerializationFormat</code>. It's kind of wrapper/abstraction around
	 * underlying Hadoop file system.
	 * 
	 * <p>
	 * The method is called by {@link HdfsWriter#write(Iterable, Class, String) HdfsWriter} prior a call to this
	 * serialization format {@link #write(Iterable, Class, String) write} method.
	 * 
	 * @param loader The loader to use.
	 */
	protected void setHdfsResourceLoader(HdfsResourceLoader loader) {
		this.hdfsResourceLoader = loader;
	}

	protected HdfsResourceLoader getHdfsResourceLoader() {
		return hdfsResourceLoader;
	}

	/**
	 * Sets the compression alias for this <code>SerializationFormat</code>. It's up to the implementation to resolve
	 * the alias to the actual compression algorithm.
	 * 
	 * <p>
	 * The method is called by {@link HdfsWriter#write(Iterable, Class, String) HdfsWriter} prior a call to this
	 * serialization format {@link #write(Iterable, Class, String) write} method.
	 * 
	 * @param codecAlias The compression alias to use.
	 */
	protected void setCompressionAlias(String compressionAlias) {
		this.compressionAlias = compressionAlias;
	}

	protected String getCompressionAlias() {
		return compressionAlias;
	}

	/**
	 * A template method writing objects to HDFS. Provides hook methods subclasses should override to encapsulate their
	 * specific logic.
	 */
	public <T> void write(Iterable<? extends T> objects, Class<T> objectsClass, String destination) throws IOException {

		HdfsResource hdfsResource;
		{
			// Append SerializationFormat extension to the destination (if not present)
			if (!destination.toLowerCase().endsWith(getExtension().toLowerCase())) {
				destination += getExtension();
			}

			// Resolve destination to HDFS Resource.
			hdfsResource = (HdfsResource) hdfsResourceLoader.getResource(destination);
		}

		// Initialization step
		doInit(objects, objectsClass, hdfsResource);

		// Core write step
		doWrite(objects, objectsClass, hdfsResource);
	}

	/**
	 * Gets the filename extension for this kind of serialization format (such as '.avro' or '.seqfile').
	 * 
	 * @return The file extension including the '.' char.
	 */
	public abstract String getExtension();

	/**
	 * A hook method for executing configuration logic prior objects write.
	 * 
	 * @param objects
	 * @param objectsClass
	 * @param hdfsResource
	 */
	protected abstract <T> void doInit(Iterable<? extends T> objects, Class<T> objectsClass, HdfsResource hdfsResource);

	/**
	 * Do the core write logic.
	 * 
	 * @param objects
	 * @param objectsClass
	 * @param hdfsResource
	 * @throws IOException
	 */
	protected abstract <T> void doWrite(Iterable<? extends T> objects, Class<T> objectsClass, HdfsResource hdfsResource)
			throws IOException;

}