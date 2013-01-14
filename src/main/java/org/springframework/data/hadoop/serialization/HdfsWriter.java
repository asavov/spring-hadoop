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
 * Utility class providing 'write to HDFS' functionality. It leverages serialization formats to do the actual objects
 * serialization and thus serve as a bridge to Hadoop HDFS.
 * 
 * @author Alex Savov
 */
public class HdfsWriter {

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
			// Open destination resource for writing.
			outputStream = createOutputStream(destination);

			write(source, outputStream);

		} catch (IOException ioExc) {
			throw new HadoopException("Cannot open output stream to '" + destination + "'", ioExc);
		} finally {
			IOUtils.closeStream(outputStream);
		}
	}

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
	 * @param serializationFormat A non-null serialization format to be used to serialize source to HDFS destination.
	 * @param destination The HDFS destination file path to write the source to.
	 * @return The output stream used to write to HDFS at provided destination.
	 * @throws IOException
	 */
	// TODO: Is it reasonable to expose HdsfOutputStreamCreator interface to the user?
	protected OutputStream createOutputStream(String destination) throws IOException {

		Assert.notNull(destination, "A non-null destination path is required.");

		// Append SerializationFormat extension to the destination (if not present)
		String extension = getSerializationFormat().getExtension();

		if (StringUtils.hasText(extension)) {
			if (!destination.toLowerCase().endsWith(extension.toLowerCase())) {
				destination += extension;
			}
		}

		// Resolve destination to HDFS Resource.
		HdfsResource hdfsDestinationResource = (HdfsResource) hdfsResourceLoader.getResource(destination);

		// Open destination resource for writing.
		return hdfsDestinationResource.getOutputStream();
	}

}
