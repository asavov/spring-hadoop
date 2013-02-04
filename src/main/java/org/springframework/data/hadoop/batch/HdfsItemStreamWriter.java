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
import org.springframework.batch.item.file.ResourceAwareItemWriterItemStream;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.fs.HdfsResource;
import org.springframework.data.hadoop.serialization.SerializationWriter;
import org.springframework.data.hadoop.serialization.SerializationWriterFactoryBean;
import org.springframework.data.hadoop.serialization.SerializationWriterObjectFactory;
import org.springframework.util.Assert;

/**
 * Multiple {@link #write(List) writes} demarcated by {@link #open(ExecutionContext) open} and {@link #close() close}
 * methods are aggregated and go to a single HDFS destination.
 * 
 * @see {@link HdfsItemWriter}
 * @see {@link HdfsMultiResourceItemWriter}
 * 
 * @author Alex Savov
 */
public class HdfsItemStreamWriter<T> extends ItemStreamSupport implements ResourceAwareItemWriterItemStream<T>,
		InitializingBean {

	private SerializationWriterObjectFactory sfObjectFactory;

	private String hdfsLocation;

	private HdfsResource hdfsResource;

	private SerializationWriter<T> serializationFormat;

	@Override
	public void open(ExecutionContext executionContext) {

		sfObjectFactory.setDestination(hdfsLocation);
		sfObjectFactory.setResource(hdfsResource);

		try {
			serializationFormat = (SerializationWriter<T>) sfObjectFactory.getObject();
		} catch (Exception e) {
			throw new ItemStreamException(e);
		}
	}

	@Override
	public void write(List<? extends T> items) throws IOException {

		// Write/Append all items to opened HDFS resource

		for (T item : items) {
			serializationFormat.write(item);
		}
	}

	@Override
	public void close() throws ItemStreamException {

		// Close serialization format

		closeStream(serializationFormat);

		serializationFormat = null;
	}

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
		hdfsLocation = location;
	}

	/**
	 * @param resource The {@link HdfsResource} instance to write to.
	 */
	@Override
	public void setResource(Resource resource) {
		Assert.isInstanceOf(HdfsResource.class, resource, "A non-null Hdfs Resource is required to write to HDFS.");

		hdfsResource = (HdfsResource) resource;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(sfObjectFactory, "A non-null SerializationWriterObjectFactory is required.");
	}

}
