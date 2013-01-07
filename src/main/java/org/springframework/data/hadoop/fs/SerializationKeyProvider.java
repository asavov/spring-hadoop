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
package org.springframework.data.hadoop.fs;

/**
 * An instance of this interface is responsible to provide a key for an object that is written to HDFS using a
 * serialization format which stores the data as key-value pairs. The object is stored as a value while the key is
 * provided by this interface.
 * 
 * @author Alex Savov
 */
// @Costin: Should we somehow utilize ConversionService/[Generic]Converter or conform to its design?
// Should we go with generic types? Any Comments?
public interface SerializationKeyProvider {

	/**
	 * Provides a key for passed object.
	 * 
	 * @param object The object for which a key should be provided.
	 * 
	 * @return The key provided for passed object. <code>null</code> if the object class is not supported.
	 */
	public Object getKey(Object object);

	/**
	 * @param objectClass The object class for which a key should be provided.
	 * 
	 * @return The key class supported by this provider for given object class. <code>null</code> otherwise.
	 */
	public Class<?> getKeyClass(Class<?> objectClass);
}