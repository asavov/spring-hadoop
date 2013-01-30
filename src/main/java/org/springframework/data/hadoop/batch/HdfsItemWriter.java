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

import static org.apache.commons.io.IOUtils.closeQuietly;

import java.util.List;

import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.fs.HdfsResource;
import org.springframework.data.hadoop.serialization.SerializationFormat;
import org.springframework.data.hadoop.serialization.SerializationFormatObjectFactory;
import org.springframework.util.Assert;

/**
 * Every {@link #write(List) write} goes to a single HDFS destination and overrides existing content.
 * 
 * @see {@link HdfsItemStreamWriter}
 * @see {@link HdfsMultiResourceItemWriter}
 * 
 * @author Alex Savov
 * 
 * @deprecated Functionally replaced by {@link HdfsItemStreamWriter}. Althought will keep it.
 */
public class HdfsItemWriter<T> implements ItemWriter<T>, InitializingBean {

	private SerializationFormatObjectFactory sfObjectFactory;

	private String hdfsDestination;

	private HdfsResource hdfsResource;

	@Override
	public void write(List<? extends T> items) throws Exception {

		sfObjectFactory.setDestination(hdfsDestination);
		sfObjectFactory.setResource(hdfsResource);

		SerializationFormat<T> sFormat = (SerializationFormat<T>) sfObjectFactory.getObject();
		try {
			for (T item : items) {
				sFormat.serialize(item);				
			}
		} finally {
			closeQuietly(sFormat);
		}
	}

	/**
	 * @param hdfsWriter The {@link SerializationFormatObjectFactory} instance used to write to underlying Hadoop file
	 * system.
	 */
	public void setSerializationFormat(SerializationFormatObjectFactory sfObjectFactory) {
		this.sfObjectFactory = sfObjectFactory;
	}

	/**
	 * @param destination The HDFS destination file path to write to.
	 */
	public void setDestination(String destination) {
		hdfsDestination = destination;
	}

	/**
	 * @param resource The {@link HdfsResource} instance to write to.
	 */
	public void setResource(Resource resource) {
		Assert.isInstanceOf(HdfsResource.class, resource, "A non-null Hdfs Resource is required to write to HDFS.");

		hdfsResource = (HdfsResource) resource;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(sfObjectFactory, "A non-null SerializationFormatObjectFactory is required.");
	}

}
