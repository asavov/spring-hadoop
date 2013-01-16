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

import org.apache.hadoop.io.IOUtils;
import org.springframework.data.hadoop.HadoopException;
import org.springframework.data.hadoop.fs.HdfsResource;
import org.springframework.data.hadoop.fs.HdfsResourceLoader;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Utility class providing 'write to HDFS' functionality. It leverages {@link SerializationFormat serialization formats}
 * to do the actual objects serialization and thus serves as a bridge to Hadoop HDFS.
 * 
 * @author Alex Savov
 */
public class HdfsWriter {

	/**
	 * Used to open HDFS resource for writing.
	 */
	protected final HdfsResourceLoader hdfsResourceLoader;

	/* The property is publicly configurable. */
	private SerializationFormat<?> serializationFormat;

	/**
	 * Constructs a new <code>HdfsWriter</code> instance.
	 * 
	 * @param hdfsResourceLoader A non-null HDFS resource loader to use.
	 */
	public HdfsWriter(HdfsResourceLoader hdfsResourceLoader) {

		Assert.notNull(hdfsResourceLoader, "A non-null HDFS resource loader is required.");

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
	 * Write source object into HDFS.
	 * 
	 * @param source The source object to write.
	 * @param destination The HDFS destination file path to write the source to.
	 */
	public <T> void write(T source, String destination) {

		OutputStream outputStream = null;
		try {
			outputStream = openOutputStream(destination);

			write(source, outputStream);
			
		} finally {
			IOUtils.closeStream(outputStream);
		}
	}

	/**
	 * Write source object into HDFS.
	 * 
	 * @param source The source object to write.
	 * @param destinationResource The HDFS destination resource to write the source to.
	 */
	public <T> void write(T source, HdfsResource destinationResource) {

		OutputStream outputStream = null;
		try {
			outputStream = openOutputStream(destinationResource);

			write(source, outputStream);

		} finally {
			IOUtils.closeStream(outputStream);
		}
	}

	/**
	 * Write source object into HDFS.
	 * 
	 * @param source The source object to write.
	 * @param outputStream The HDFS output stream to write the source to.
	 */
	public <T> void write(T source, OutputStream outputStream) {
		if (source == null) {
			// Silently return...
			return;
		}

		SerializationFormat<?> serializationFormat = getSerializationFormat();

		Assert.notNull(serializationFormat, "A non-null serialization format is required.");

		try {
			// Delegate to core SerializationFormat logic.
			((SerializationFormat<T>) serializationFormat).serialize(source, outputStream);
		} catch (IOException ioExc) {
			throw new HadoopException("Cannot write the source object to HDFS: " + ioExc.getMessage(), ioExc);
		}
	}

	/**
	 * @param destination The HDFS destination file path to write to.
	 * @return The output stream used to write to HDFS at provided destination.
	 */
	public OutputStream openOutputStream(String destination) {

		destination = canonicalSerializationDestination(destination);

		HdfsResource hdfsResource = (HdfsResource) hdfsResourceLoader.getResource(destination);

		return openOutputStream(hdfsResource);
	}

	/**
	 * @param destinationResource The HDFS destination resource to write to.
	 * @return The output stream used to write to HDFS at provided destination.
	 */
	public OutputStream openOutputStream(HdfsResource destinationResource) {

		destinationResource = canonicalSerializationDestination(destinationResource);

		try {
			// Open destination resource for writing.
			return destinationResource.getOutputStream();
		} catch (IOException ioExc) {
			throw new HadoopException("Cannot open output stream to '" + destinationResource + "'", ioExc);
		}
	}

	protected HdfsResource canonicalSerializationDestination(HdfsResource destinationResource) {

		Assert.notNull(destinationResource, "A non-null destination resource is required.");

		String destination = destinationResource.getFilename();

		if (!isCanonicalSerializationDestination(destination)) {

			destination = canonicalSerializationDestination(destination);

			destinationResource = (HdfsResource) hdfsResourceLoader.getResource(destination);
		}

		return destinationResource;
	}

	protected String canonicalSerializationDestination(String destination) {

		Assert.notNull(destination, "A non-null destination is required.");

		if (!isCanonicalSerializationDestination(destination)) {
			
			destination += getSerializationFormat().getExtension();
		}

		return destination;
	}

	protected boolean isCanonicalSerializationDestination(String destination) {

		Assert.notNull(destination, "A non-null destination is required.");

		String extension = getSerializationFormat().getExtension();

		return StringUtils.hasText(extension) ? destination.toLowerCase().endsWith(extension.toLowerCase()) : true;
	}

}
