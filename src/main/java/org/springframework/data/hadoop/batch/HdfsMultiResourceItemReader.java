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

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.hadoop.fs.HdfsResourceLoader;
import org.springframework.util.Assert;

/**
 * Spring Batch {@link ItemReader} implementation for reading data from multiple HDFS resources.
 * 
 * <p>This class exists as it has to do the conversion manually since there's no pluggable
 * way to add another resource loader.
 * 
 * @author Alex Savov
 */
public class HdfsMultiResourceItemReader<T> extends MultiResourceItemReader<T> implements
		InitializingBean {

	/* Used to resolve location pattern to resources. */
	private HdfsResourceLoader loader;

	/* HDFS location pattern to read from. */
	private String locationPattern;

	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		if (hasText(locationPattern)) {
			try {
				setResources(loader.getResources(locationPattern));
			} catch (IOException exc) {
				throw new ItemStreamException("Could not resolve location pattern '" + locationPattern + "'.", exc);
			}
		}
		
		super.open(executionContext);
	}

	/**
	 * @param locationPattern The HDFS location pattern to read from.
	 */
	public void setLocationPattern(String locationPattern) {
		this.locationPattern = locationPattern;
	}

	/**
	 * @param hdfsResourceLoader The {@link HdfsResourceLoader} instance used to resolve location pattern to HDFS
	 * resources.
	 */
	public void setHdfsResourceLoader(HdfsResourceLoader hdfsResourceLoader) {
		this.loader = hdfsResourceLoader;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(loader, "A non-null HdfsResourceLoader is required.");
	}

}
