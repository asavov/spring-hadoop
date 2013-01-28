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
 * Utility class providing 'write to HDFS' functionality. It leverages {@link SerializationFormat serialization formats}
 * to do the actual objects serialization and thus serves as a bridge to Hadoop HDFS.
 * 
 * @author Alex Savov
 */
public class SerializationFormatObjectFactory implements ObjectFactory<SerializationFormat<?>> {

	/* Used to open HDFS resource for writing. */
	protected final HdfsResourceLoader hdfsResourceLoader;

	private SerializationFormatCreator<?> serializationCreator;

	/* The property is publicly configurable. */
	private String destinationPath;

	/* The property is publicly configurable. */
	private HdfsResource destinationResource;

	/**
	 * Constructs a new <code>SerializationFormatObjectFactory</code> instance.
	 * 
	 * @param hdfsResourceLoader A non-null HDFS resource loader to use.
	 */
	public SerializationFormatObjectFactory(HdfsResourceLoader hdfsResourceLoader) {

		Assert.notNull(hdfsResourceLoader, "A non-null HDFS resource loader is required.");

		this.hdfsResourceLoader = hdfsResourceLoader;
	}

	/**
	 * @param destinationPath the destinationPath to set
	 */
	public void setDestination(String destinationPath) {
		this.destinationPath = destinationPath;
	}

	/**
	 * @param destinationResource the destinationResource to set
	 */
	public void setResource(HdfsResource destinationResource) {
		this.destinationResource = destinationResource;
	}

	/**
	 * @param serializationFormatFactory the serializationFormatFactory to set
	 */
	public void setSerializationFormatCreator(SerializationFormatCreator<?> serializationCreator) {
		this.serializationCreator = serializationCreator;
	}

	@Override
	public SerializationFormat<?> getObject() {

		Assert.notNull(serializationCreator, "A non-null SerializationFormatCreator is required.");

		OutputStream outputStream = null;

		if (destinationResource != null) {

			outputStream = openOutputStream(serializationCreator, destinationResource);

		} else if (hasText(destinationPath)) {

			outputStream = openOutputStream(serializationCreator, destinationPath);

		} else {
			Assert.state(false, "Set either 'destinationPath' or 'destinationResource' property.");
		}

		return serializationCreator.createSerializationFormat(outputStream);
	}

	/**
	 * @param destination The HDFS destination file path to write to.
	 * @return The output stream used to write to HDFS at provided destination.
	 */
	protected OutputStream openOutputStream(SerializationFormatCreator<?> serializationFactory, String destination) {

		destination = canonicalSerializationDestination(serializationFactory, destination);

		HdfsResource hdfsResource = (HdfsResource) hdfsResourceLoader.getResource(destination);

		return openOutputStream(serializationFactory, hdfsResource);
	}

	/**
	 * @param destinationResource The HDFS destination resource to write to.
	 * @return The output stream used to write to HDFS at provided destination.
	 */
	protected OutputStream openOutputStream(SerializationFormatCreator<?> serializationFactory,
			HdfsResource destinationResource) {

		destinationResource = canonicalSerializationDestination(serializationFactory, destinationResource);

		try {
			// Open destination resource for writing.
			return destinationResource.getOutputStream();
		} catch (IOException ioExc) {
			throw new HadoopException("Cannot open output stream to '" + destinationResource + "'", ioExc);
		}
	}

	protected HdfsResource canonicalSerializationDestination(SerializationFormatCreator<?> serializationFactory,
			HdfsResource destinationResource) {

		String destination = destinationResource.getFilename();

		if (!isCanonicalSerializationDestination(serializationFactory, destination)) {

			destination = canonicalSerializationDestination(serializationFactory, destination);

			destinationResource = (HdfsResource) hdfsResourceLoader.getResource(destination);
		}

		return destinationResource;
	}

	protected String canonicalSerializationDestination(SerializationFormatCreator<?> serializationFactory,
			String destination) {

		if (!isCanonicalSerializationDestination(serializationFactory, destination)) {

			destination += serializationFactory.getExtension();
		}

		return destination;
	}

	protected boolean isCanonicalSerializationDestination(SerializationFormatCreator<?> serializationFactory,
			String destination) {

		String extension = serializationFactory.getExtension();

		return !StringUtils.hasText(extension) || destination.toLowerCase().endsWith(extension.toLowerCase());
	}

}
