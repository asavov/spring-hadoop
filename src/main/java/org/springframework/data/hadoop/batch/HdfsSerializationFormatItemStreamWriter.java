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

import static org.apache.hadoop.io.IOUtils.closeStream;

import java.io.IOException;
import java.util.List;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.ResourceAwareItemWriterItemStream;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.fs.HdfsResource;
import org.springframework.data.hadoop.serialization.SerializationWriter;
import org.springframework.data.hadoop.serialization.SerializationWriterFactoryBean;
import org.springframework.data.hadoop.serialization.SerializationWriterObjectFactory;
import org.springframework.util.Assert;

/**
 * Spring Batch {@link ItemWriter} implementation for writing data to Hadoop using Hadoop serialization formats.
 * Multiple {@link #write(List) writes} demarcated by {@link #open(ExecutionContext) open} and {@link #close() close}
 * methods are aggregated and go to a single HDFS destination.
 * 
 * @see {@link SerializationFormat}
 * @see {@link HdfsSerializationFormatItemWriter}
 * @see {@link HdfsSerializationFormatMultiResourceItemWriter}
 * 
 * @author Alex Savov
 */
public class HdfsSerializationFormatItemStreamWriter<T> extends ItemStreamSupport implements ResourceAwareItemWriterItemStream<T>,
		InitializingBean {

	/* The Writer provides core 'write objects to Hadoop' logic. Its lifecycle is demarcated by 'open-close' methods. */
	private SerializationWriter<T> serializationWriter;

	// The properties are publicly configurable.

	/* HDFS location to write to. */
	private String location;

	/* HDFS resource to write to. */
	private HdfsResource resource;

	/* The factory used to open/create serialization writers to passed HDFS destination. */
	private SerializationWriterObjectFactory sfObjectFactory;

	//
	// Adapt Serialization Writer to Spring Batch Item Writer contract {{
	//

	/**
	 * Create Serialization Writer with passed parameters.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void open(ExecutionContext executionContext) {

		sfObjectFactory.setDestination(location);
		sfObjectFactory.setResource(resource);

		serializationWriter = (SerializationWriter<T>) sfObjectFactory.getObject();
	}

	/**
	 * Write all items to opened Serialization Writer.
	 */
	@Override
	public void write(List<? extends T> items) throws IOException {
		for (T item : items) {
			serializationWriter.write(item);
		}
	}

	/**
	 * Close the Serialization Writer.
	 */
	@Override
	public void close() {

		closeStream(serializationWriter);

		serializationWriter = null;
	}

	// }}

	/**
	 * @param hdfsWriter The {@link SerializationWriterFactoryBean} instance used to write to underlying Hadoop file
	 * system.
	 */
	public void setSerializationFormat(SerializationWriterObjectFactory sfObjectFactory) {
		this.sfObjectFactory = sfObjectFactory;
	}

	/**
	 * @param location The HDFS destination file path to write to.
	 */
	public void setLocation(String location) {
		this.location = location;
	}

	/**
	 * @param resource The {@link HdfsResource} instance to write to.
	 */
	@Override
	public void setResource(Resource resource) {
		Assert.isInstanceOf(HdfsResource.class, resource, "A non-null Hdfs Resource is required to write to HDFS.");

		this.resource = (HdfsResource) resource;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(sfObjectFactory, "A non-null SerializationWriterObjectFactory is required.");
	}

}
