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
package org.springframework.data.hadoop.serialization;

/**
 * The class provides support needed by {@link SerializationFormat} implementations that serialize a collection of Java
 * objects.
 * 
 * @author Alex Savov
 */
// TODO: The class is introduced ONLY to provide Class information about the collection of objects to write.
// Maybe we might somehow benefit from org.springframework.data.util.TypeInformation support. Needs investigation.
public abstract class AbstractObjectsSerializationFormat<T> extends SerializationFormatSupport<Iterable<? extends T>> {

	/* The class of the objects that are serialized by this format. */
	protected final Class<T> objectsClass;

	/**
	 * @param objectsClass The class of the objects that are serialized by this format.
	 */
	protected AbstractObjectsSerializationFormat(Class<T> objectsClass) {
		this.objectsClass = objectsClass;
	}

}