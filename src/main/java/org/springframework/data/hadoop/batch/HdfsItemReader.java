/*
 * Copyright 2011-2012 the original author or authors.
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
package org.springframework.data.hadoop.batch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.fs.HdfsResourceLoader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Spring Batch {@link ItemReader} implementation for Hadoop {@link FileSystem}.
 * 
 * <p>This class exists as it has to do the conversion manually since there's no pluggable
 * way to add another resource loader.
 *
 * @param <T> the generic type
 * @author Costin Leau
 */
public class HdfsItemReader<T> extends FlatFileItemReader<T> {

	private Resource resource;
	private HdfsResourceLoader loader;
	private String location;

	/**
	 * Instantiates a new hdfs item reader.
	 *
	 * @param configuration the configuration
	 */
	public HdfsItemReader(Configuration configuration) {
		this(new HdfsResourceLoader(configuration));
	}

	/**
	 * Instantiates a new hdfs item reader.
	 *
	 * @param fs the fs
	 */
	public HdfsItemReader(FileSystem fs) {
		this(new HdfsResourceLoader(fs));
	}


	/**
	 * Instantiates a new hdfs item reader.
	 *
	 * @param hdfsLoader the hdfs loader
	 */
	public HdfsItemReader(HdfsResourceLoader hdfsLoader) {
		Assert.notNull(hdfsLoader, "a valid resource loader is required");
		this.loader = hdfsLoader;
		setName(ClassUtils.getShortName(getClass()));
	}


	@Override
	protected void doOpen() throws Exception {		
		// @Costin: this is the private 'resource' which is NEVER set.
		if (resource == null) {
			Assert.hasText(location, "either a resource or a location need to be specified");
			resource = loader.getResource(location);
			// @Costin: the check is obsolete cause the parent class provides even better handling of checked state.
			Assert.isTrue(resource.exists() && resource.isReadable(), "non-existing or non-readable resource " + resource);
		}

		setResource(resource);
		super.doOpen();
		
		/* @Costin: how about that version ?
		if (hasText(location)) {
			setResource(loader.getResource(location));
		}		
		super.doOpen();
		*/
	}

	/**
	 * Sets the location.
	 *
	 * @param location the new location
	 */
	public void setLocation(String location) {
		this.location = location;
	}

}