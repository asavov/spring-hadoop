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

import java.io.IOException;
import java.util.List;

import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.fs.HdfsResource;
import org.springframework.data.hadoop.serialization.SerializationFormat;
import org.springframework.data.hadoop.serialization.SerializationFormatOperations.SerializationWriterCallback;
import org.springframework.data.hadoop.serialization.SerializationFormatTemplate;
import org.springframework.data.hadoop.serialization.SerializationWriter;
import org.springframework.data.hadoop.serialization.SerializationWriterObjectFactory;
import org.springframework.util.Assert;

/**
 * Spring Batch {@link ItemWriter} implementation for writing data to Hadoop using Hadoop serialization formats. Every
 * {@link #write(List) write} goes to a single HDFS destination and overrides existing content.
 * 
 * @see {@link SerializationFormat}
 * @see {@link HdfsSerializationFormatItemStreamWriter}
 * @see {@link HdfsSerializationFormatMultiResourceItemWriter}
 * 
 * @author Alex Savov
 */
public class HdfsSerializationFormatItemWriter<T> implements ItemWriter<T>, InitializingBean {

	// The properties are publicly configurable.

	/* HDFS location to write to. */
	private String location;

	/* HDFS resource to write to. */
	private HdfsResource resource;

	/* The factory used to open/create serialization writers to passed HDFS destination. */
	private SerializationWriterObjectFactory sfObjectFactory;

	@Override
	public void write(final List<? extends T> items) throws Exception {

		sfObjectFactory.setResource(resource);
		
		new SerializationFormatTemplate(sfObjectFactory).write(location, new SerializationWriterCallback<T>() {
			@Override
			public void doInSerializationFormat(SerializationWriter<T> serializationFormat) throws IOException {
				for (T item : items) {
					serializationFormat.write(item);
				}
			}
		});
	}

	/**
	 * @param sfObjectFactory The {@link SerializationWriterObjectFactory} instance used to write to Hadoop.
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
	public void setResource(Resource resource) {
		Assert.isInstanceOf(HdfsResource.class, resource, "A non-null Hdfs Resource is required to write to HDFS.");

		this.resource = (HdfsResource) resource;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(sfObjectFactory, "A non-null SerializationWriterObjectFactory is required.");
	}

}
