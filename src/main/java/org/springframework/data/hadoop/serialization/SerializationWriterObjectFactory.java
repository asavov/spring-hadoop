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

import static org.springframework.util.StringUtils.hasText;

import java.io.IOException;
import java.io.OutputStream;

import org.springframework.beans.factory.ObjectFactory;
import org.springframework.data.hadoop.HadoopException;
import org.springframework.data.hadoop.fs.HdfsResource;
import org.springframework.data.hadoop.fs.HdfsResourceLoader;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * A factory class (conforming to Spring {@link ObjectFactory} API) responsible to create {@link SerializationWriter}
 * instances.
 * 
 * <p>
 * The class accepts HDFS path (as String) or HDFS {@link HdfsResource resource} as destination for the write in
 * contrast to {@link SerializationFormat} which accepts the low-level <code>OutputStream</code>. Internally it appends
 * serialization format {@link SerializationFormat#getExtension() extension} to passed destination (if needed), opens an
 * output stream to it and delegates serialization format creation to
 * {@link SerializationFormat#getWriter(OutputStream) SerializationFormat}.
 * 
 * @author Alex Savov
 */
public class SerializationWriterObjectFactory implements ObjectFactory<SerializationWriter<?>> {

	/* Used to open HDFS resource for writing. */
	protected final HdfsResourceLoader hdfsResourceLoader;

	// The properties are publicly configurable. See setters for details.

	protected SerializationFormat<?> serializationCreator;

	protected String hdfsDestinationPath;

	protected HdfsResource hdfsDestinationResource;

	/**
	 * Constructs a new <code>SerializationWriterObjectFactory</code> instance.
	 * 
	 * @param hdfsResourceLoader A non-null HDFS resource loader to use.
	 */
	public SerializationWriterObjectFactory(HdfsResourceLoader hdfsResourceLoader) {

		Assert.notNull(hdfsResourceLoader, "A non-null HDFS resource loader is required.");

		this.hdfsResourceLoader = hdfsResourceLoader;
	}

	/**
	 * The serialization format returned by {@link #getObject()} writes to this HDFS destination file path if set.
	 * Setting of the {@link #setResource(HdfsResource) resource} property is higher priority.
	 * 
	 * @param destinationPath The HDFS destination file path to write to.
	 */
	public void setDestination(String destinationPath) {
		this.hdfsDestinationPath = destinationPath;
	}

	/**
	 * The serialization format returned by {@link #getObject()} writes to this HDFS destination resource if set.
	 * 
	 * @param destinationResource The HDFS destination resource to write to.
	 */
	public void setResource(HdfsResource destinationResource) {
		this.hdfsDestinationResource = destinationResource;
	}

	/**
	 * The creation of the serialization format returned by {@link #getObject()} is delegated to this instance.
	 * 
	 * @param serializationCreator The <code>SerializationFormat</code> used by this class to create serialization
	 * format instances.
	 */
	public void setSerializationFormatCreator(SerializationFormat<?> serializationCreator) {
		this.serializationCreator = serializationCreator;
	}

	/**
	 * Appends serialization format {@link SerializationFormat#getExtension() extension} to passed destination (if
	 * needed), opens an output stream to it and delegates serialization format creation to
	 * {@link SerializationFormat#getWriter(OutputStream) SerializationFormat}
	 * 
	 * @return SerializationFormat instance which writes either to HDFS {@link HdfsResource resource} or HDFS path.
	 */
	@Override
	public SerializationWriter<?> getObject() {

		Assert.notNull(serializationCreator, "A non-null SerializationFormat is required.");

		OutputStream outputStream = null;

		if (hdfsDestinationResource != null) {

			outputStream = openOutputStream(serializationCreator, hdfsDestinationResource);

		} else if (hasText(hdfsDestinationPath)) {

			outputStream = openOutputStream(serializationCreator, hdfsDestinationPath);

		} else {
			Assert.state(false, "Set either 'destinationPath' or 'destinationResource' property.");
		}

		return serializationCreator.getWriter(outputStream);
	}

	/**
	 * @param serializationCreator
	 * @param destination The HDFS destination file path to write to.
	 * @return The output stream used to write to HDFS at provided destination.
	 */
	protected OutputStream openOutputStream(SerializationFormat<?> serializationCreator, String destination) {

		destination = canonicalSerializationDestination(serializationCreator, destination);

		HdfsResource hdfsResource = (HdfsResource) hdfsResourceLoader.getResource(destination);

		return openOutputStream(serializationCreator, hdfsResource);
	}

	/**
	 * @param serializationCreator
	 * @param destinationResource The HDFS destination resource to write to.
	 * @return The output stream used to write to HDFS at provided destination.
	 */
	protected OutputStream openOutputStream(SerializationFormat<?> serializationCreator,
			HdfsResource destinationResource) {

		destinationResource = canonicalSerializationDestination(serializationCreator, destinationResource);

		try {
			// Open destination resource for writing.
			return destinationResource.getOutputStream();
		} catch (IOException ioExc) {
			throw new HadoopException("Cannot open output stream to '" + destinationResource + "'", ioExc);
		}
	}

	/**
	 * @param serializationCreator
	 * @param destinationResource The HDFS destination resource to write to.
	 * @return passed <code>destinationResource</code> if its filename ends with serialization format extension.
	 * Otherwise return a new <code>HdfsResource</code> with a filename which is a concatenation of
	 * <code>destinationResource</code> filename and the serialization format extension.
	 */
	protected HdfsResource canonicalSerializationDestination(SerializationFormat<?> serializationCreator,
			HdfsResource destinationResource) {

		String destination = destinationResource.getFilename();

		if (!isCanonicalSerializationDestination(serializationCreator, destination)) {

			destination = canonicalSerializationDestination(serializationCreator, destination);

			destinationResource = (HdfsResource) hdfsResourceLoader.getResource(destination);
		}

		return destinationResource;
	}

	/**
	 * @param serializationCreator
	 * @param destination The HDFS destination file path to write to.
	 * @return passed <code>destination</code> if it ends with serialization format extension. Otherwise return a new
	 * destination which is a concatenation of <code>destination</code> and the serialization format extension.
	 */
	protected String canonicalSerializationDestination(SerializationFormat<?> serializationCreator, String destination) {

		if (!isCanonicalSerializationDestination(serializationCreator, destination)) {

			destination += serializationCreator.getExtension();
		}

		return destination;
	}

	/**
	 * @param serializationCreator
	 * @param destination HDFS destination file path.
	 * @return <code>true</code> if <code>destination</code> ends with serialization format extension;
	 * <code>false</code> otherwise.
	 */
	protected boolean isCanonicalSerializationDestination(SerializationFormat<?> serializationCreator,
			String destination) {

		String extension = serializationCreator.getExtension();

		return !StringUtils.hasText(extension) || destination.toLowerCase().endsWith(extension.toLowerCase());
	}

}
