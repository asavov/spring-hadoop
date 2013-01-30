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

import org.springframework.beans.factory.FactoryBean;
import org.springframework.data.hadoop.fs.HdfsResourceLoader;

/**
 * A factory class (conforming to Spring {@link FactoryBean} API) responsible to create {@link SerializationFormat}
 * instances.
 * 
 * @see {@link SerializationFormatObjectFactory}
 * 
 * @author Alex Savov
 */
public class SerializationFormatFactoryBean extends SerializationFormatObjectFactory implements
		FactoryBean<SerializationFormat<?>> {

	/**
	 * Constructs a new <code>SerializationFormatFactoryBean</code> instance.
	 * 
	 * @param hdfsResourceLoader A non-null HDFS resource loader to use.
	 */
	public SerializationFormatFactoryBean(HdfsResourceLoader hdfsResourceLoader) {
		super(hdfsResourceLoader);
	}

	/**
	 * @return <code>SerializationFormat.class</code>
	 */
	@Override
	public Class<?> getObjectType() {
		return SerializationFormat.class;
	}

	/**
	 * @return <code>false</code> since a new instance is returned by {@link #getObject()} method on every call.
	 */
	@Override
	public boolean isSingleton() {
		return false;
	}

}
