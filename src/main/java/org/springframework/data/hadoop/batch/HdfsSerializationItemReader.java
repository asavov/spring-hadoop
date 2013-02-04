/*
 * Copyright 2004-2012 the original author or authors.
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

import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.serialization.SerializationFormat;
import org.springframework.data.hadoop.serialization.SerializationReader;
import org.springframework.util.Assert;

/**
 * 
 * @author asavov
 */
public class HdfsSerializationItemReader<T> extends AbstractItemCountingItemStreamItemReader<T> implements ResourceAwareItemReaderItemStream<T>, InitializingBean {

	private SerializationFormat<T> serializationFormat;

	private String location;

	private Resource resource;
	
	private SerializationReader<T> serializationReader;

	/**
	 * @param location The HDFS destination file path to read from.
	 */
	public void setLocation(String location) {
		this.location = location;
	}

	/**
	 * @param resource The {@link Resource} instance to read from.
	 */
	public void setResource(Resource resource) {
		this.resource = resource;
	}

	/**
	 * @param serializationFormat The {@link SerializationFormat} instance used to read from underlying Hadoop file
	 * system.
	 */
	public void setSerializationFormat(SerializationFormat<T> serializationFormat) {
		this.serializationFormat = serializationFormat;
	}

	@Override
	protected void doOpen() throws IOException {
		serializationReader = serializationFormat.getReader(location);
	}
	
	@Override
	protected T doRead() throws IOException {
		return serializationReader.read();
	}

	@Override
	protected void doClose() throws IOException {
		serializationReader.close();
	}
	
	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(serializationFormat, "A non-null SerializationFormat is required.");
	}
	

}
