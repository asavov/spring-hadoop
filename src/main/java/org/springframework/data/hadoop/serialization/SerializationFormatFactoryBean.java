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

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
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
public class SerializationFormatFactoryBean<T> implements FactoryBean<SerializationFormat<T>> {

	/* Used to create SerializationFormat prototype instances. */
	@Autowired
	private ApplicationContext ctx;

	/* Used to open HDFS resource for writing. */
	protected final HdfsResourceLoader hdfsResourceLoader;

	/* The property is publicly configurable. */
	private String serializationFormatBeanName;

	private SerializationFormat<T> serializationFormat;

	/* The property is publicly configurable. */
	private String destinationPath;

	/* The property is publicly configurable. */
	private HdfsResource destinationResource;

	/**
	 * Constructs a new <code>HdfsWriter</code> instance.
	 * 
	 * @param hdfsResourceLoader A non-null HDFS resource loader to use.
	 */
	public SerializationFormatFactoryBean(HdfsResourceLoader hdfsResourceLoader) {

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
	 * @param serializationFormatBeanName the serializationFormatBeanName to set
	 */
	public void setSerializationFormatBeanName(String serializationFormatBeanName) {
		this.serializationFormatBeanName = serializationFormatBeanName;
	}

	public void setSerializationFormat(SerializationFormat<T> serializationFormat) {
		this.serializationFormat = serializationFormat;
	}

	@Override
	public SerializationFormat<T> getObject() throws Exception {

		SerializationFormat<T> sFormat = (serializationFormat != null) ? serializationFormat : ctx.getBean(
				serializationFormatBeanName, SerializationFormat.class);

		OutputStream outputStream = null;

		if (destinationResource != null) {

			outputStream = openOutputStream(sFormat, destinationResource);

		} else if (hasText(destinationPath)) {

			outputStream = openOutputStream(sFormat, destinationPath);

		} else {
			Assert.state(false, "Set either 'destinationPath' or 'destinationResource' property.");
		}

		sFormat.setOutputStream(outputStream);
		
		return sFormat;
	}

	@Override
	public Class<?> getObjectType() {
		return SerializationFormat.class;
	}

	@Override
	public boolean isSingleton() {
		return false;
	}

	/**
	 * @param destination The HDFS destination file path to write to.
	 * @return The output stream used to write to HDFS at provided destination.
	 */
	protected OutputStream openOutputStream(SerializationFormat<?> serialization, String destination) {

		destination = canonicalSerializationDestination(serialization, destination);

		HdfsResource hdfsResource = (HdfsResource) hdfsResourceLoader.getResource(destination);

		return openOutputStream(serialization, hdfsResource);
	}

	/**
	 * @param destinationResource The HDFS destination resource to write to.
	 * @return The output stream used to write to HDFS at provided destination.
	 */
	protected OutputStream openOutputStream(SerializationFormat<?> serialization, HdfsResource destinationResource) {

		destinationResource = canonicalSerializationDestination(serialization, destinationResource);

		try {
			// Open destination resource for writing.
			return destinationResource.getOutputStream();
		} catch (IOException ioExc) {
			throw new HadoopException("Cannot open output stream to '" + destinationResource + "'", ioExc);
		}
	}

	protected HdfsResource canonicalSerializationDestination(SerializationFormat<?> serialization,
			HdfsResource destinationResource) {

		Assert.notNull(destinationResource, "A non-null destination resource is required.");

		String destination = destinationResource.getFilename();

		if (!isCanonicalSerializationDestination(serialization, destination)) {

			destination = canonicalSerializationDestination(serialization, destination);

			destinationResource = (HdfsResource) hdfsResourceLoader.getResource(destination);
		}

		return destinationResource;
	}

	protected String canonicalSerializationDestination(SerializationFormat<?> serialization, String destination) {

		Assert.notNull(destination, "A non-null destination is required.");

		if (!isCanonicalSerializationDestination(serialization, destination)) {

			destination += serialization.getExtension();
		}

		return destination;
	}

	protected boolean isCanonicalSerializationDestination(SerializationFormat<?> serialization, String destination) {

		Assert.notNull(destination, "A non-null destination is required.");

		String extension = serialization.getExtension();

		return StringUtils.hasText(extension) ? destination.toLowerCase().endsWith(extension.toLowerCase()) : true;
	}

}
