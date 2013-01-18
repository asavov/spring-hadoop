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

import java.util.List;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.item.file.MultiResourceItemWriter;
import org.springframework.batch.item.file.ResourceSuffixCreator;
import org.springframework.batch.item.file.SimpleResourceSuffixCreator;
import org.springframework.batch.item.util.ExecutionContextUserSupport;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Multiple {@link #write(List) writes} demarcated by {@link #open(ExecutionContext) open} and {@link #close() close}
 * methods go to separate HDFS destinations.
 * 
 * <p>
 * Impl note: The class mimics {@link MultiResourceItemWriter}. Unfortunately it could not re-use it cause it's coupled
 * to <code>java.io.File</code> abstraction which is not applicable in HDFS case.
 * 
 * @see {@link HdfsItemWriter}
 * @see {@link HdfsItemStreamWriter}
 * 
 * @author Alex Savov
 */
public class HdfsMultiResourceItemWriter<T> extends ExecutionContextUserSupport implements ItemStreamWriter<T>,
		InitializingBean {

	private final static String RESOURCE_INDEX_KEY = "resource.index";

	private String baseResource;

	private int resourceIndex = -1;

	private ResourceSuffixCreator suffixCreator = new SimpleResourceSuffixCreator();

	private HdfsItemWriter<T> delegate;

	public HdfsMultiResourceItemWriter() {
		setName(ClassUtils.getShortName(HdfsMultiResourceItemWriter.class));
	}

	public void write(List<? extends T> items) throws Exception {

		String destination = baseResource + suffixCreator.getSuffix(resourceIndex++);

		delegate.setDestination(destination);

		delegate.write(items);
	}

	/**
	 * Every {@link #write(List) write} is delegated to that instance.
	 */
	public void setDelegate(HdfsItemWriter<T> delegate) {
		this.delegate = delegate;
	}

	/**
	 * Prototype for HDFS destination file path. The prototype will be appended with a suffix (according to
	 * {@link #setResourceSuffixCreator(ResourceSuffixCreator)} to build the actual paths.
	 */
	public void setBaseDestination(String baseDestination) {
		this.baseResource = baseDestination;
	}

	/**
	 * Customize the suffix of the HDFS destination file path to which every
	 * {@link HdfsMultiResourceItemWriter#write(List) write} goes. The suffix returned is appended to provided
	 * {@link #setBaseDestination(String) base destination}.
	 */
	public void setResourceSuffixCreator(ResourceSuffixCreator resourceSuffixCreator) {
		this.suffixCreator = resourceSuffixCreator;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(delegate, "A non-null HdfsItemWriter is required.");
		Assert.notNull(suffixCreator, "A non-null ResourceSuffixCreator is required.");
	}

	@Override
	public void open(ExecutionContext executionContext) {
		resourceIndex = executionContext.getInt(getKey(RESOURCE_INDEX_KEY), 0);
	}

	@Override
	public void update(ExecutionContext executionContext) {
		executionContext.putInt(getKey(RESOURCE_INDEX_KEY), resourceIndex);
	}

	@Override
	public void close() {
		resourceIndex = -1;
	}

}
