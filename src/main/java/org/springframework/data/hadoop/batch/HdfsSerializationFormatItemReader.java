/*
 * Copyright 2013 the original author or authors.
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

package org.springframework.data.hadoop.batch;

import static org.springframework.util.StringUtils.hasText;

import java.io.IOException;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.serialization.SerializationFormat;
import org.springframework.data.hadoop.serialization.SerializationReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Spring Batch {@link ItemReader} implementation for reading data from a single HDFS resource using serialization
 * format.
 * 
 * @see {@link SerializationFormat}
 * 
 * @author Alex Savov
 */
public class HdfsSerializationFormatItemReader<T> extends AbstractItemCountingItemStreamItemReader<T> implements
		ResourceAwareItemReaderItemStream<T>, InitializingBean {

	/* The Reader provides core 'read objects from Hadoop' logic. Its lifecycle is demarcated by 'open-close' methods. */
	private SerializationReader<T> serializationReader;

	// The properties are publicly configurable.

	/* The HDFS serialization format used to read objects. */
	private SerializationFormat<T> serializationFormat;

	/* HDFS location to read from. */
	private String location;

	/* HDFS resource to read from. */
	private Resource resource;

	{
		/* Initialize the name for the key in the execution context. */
		setName(ClassUtils.getShortName(HdfsSerializationFormatItemReader.class));
	}

	//
	// Adapt Serialization Reader to Spring Batch Item Reader contract {{
	//

	@Override
	protected void doOpen() throws IOException {
		if (hasText(location)) {

			serializationReader = serializationFormat.getReader(location);

		} else if (resource != null) {

			// Passed resource is used only to get its URI.
			serializationReader = serializationFormat.getReader(resource.getURI().toString());

		} else {
			Assert.state(false, "Set either 'location' or 'resource' property.");
		}
	}

	@Override
	protected T doRead() throws IOException {
		return serializationReader.read();
	}

	@Override
	protected void doClose() throws IOException {
		serializationReader.close();
		serializationReader = null;
	}

	// }}

	/**
	 * @param location The HDFS destination file path to read from.
	 */
	public void setLocation(String location) {
		this.location = location;
	}

	/**
	 * @param resource The {@link Resource} instance to read from.
	 */
	@Override
	public void setResource(Resource resource) {
		this.resource = resource;
	}

	/**
	 * @param serializationFormat The {@link SerializationFormat} instance used to read objects from Hadoop.
	 */
	public void setSerializationFormat(SerializationFormat<T> serializationFormat) {
		this.serializationFormat = serializationFormat;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(serializationFormat, "A non-null SerializationFormat is required.");
	}

}
