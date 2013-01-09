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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import org.springframework.core.MethodParameter;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.data.hadoop.HadoopException;
import org.springframework.expression.AccessException;
import org.springframework.expression.spel.support.ReflectivePropertyAccessor;
import org.springframework.util.Assert;

/**
 * Reflective implementation of {@link SerializationKeyProvider} returning specific object property as object key. A
 * property is accessible through a getter method or a field of the object.
 * 
 * @author Alex Savov
 */
public class ReflectiveSerializationKeyProvider extends ReflectivePropertyAccessor implements SerializationKeyProvider {

	/* The name of the property (either getter method or field) to use as key. */
	private final String propertyName;

	/* The class of the object for which a key should be provided. */
	private final Class<?> objectClass;

	/* A metadata for the key property (either getter method or field). */
	private final TypeDescriptor keyDescriptor;

	/**
	 * @param objectClass The class of the object for which a key should be provided.
	 * @param propertyName The name of the property (either getter method or field) to use as key.
	 */
	public ReflectiveSerializationKeyProvider(Class<?> objectClass, String propertyName) {

		// Try to resolve from getter...
		Method getter = findGetterForProperty(propertyName, objectClass, false);
		if (getter != null) {
			keyDescriptor = new TypeDescriptor(new MethodParameter(getter, -1));
		} else {
			// Try to resolve from field
			Field field = findField(propertyName, objectClass, false);
			if (field != null) {
				keyDescriptor = new TypeDescriptor(field);
			} else {
				throw new HadoopException("Neither getter method nor field found for property '" + propertyName
						+ "' of '" + objectClass + "'.");
			}
		}

		this.objectClass = objectClass;
		this.propertyName = propertyName;
	}

	/**
	 * @return The value of the key property, which is either getter method or field on the object.
	 */
	public Object getKey(Object object) {
		Assert.notNull(getKeyClass(object.getClass()),
				"This ReflectiveSerializationKeyProvider provides keys only for objects of " + objectClass);

		try {
			return read(null, object, propertyName).getValue();
		} catch (AccessException e) {
			throw new HadoopException(e.getMessage(), e);
		}
	}

	/**
	 * Keys are provided only for the object class that's specified in the constructor.
	 * 
	 * @return The class of the key property (either getter method or field). <code>null</code> if passed object class
	 * is not supported by this provider.
	 */
	public Class<?> getKeyClass(Class<?> objectClass) {
		return (this.objectClass == objectClass) ? keyDescriptor.getObjectType() : null;
	}

	/**
	 * Find a declared field (as specified by {@link Class#getDeclaredField(String)} contract) of a certain name on a
	 * specified class.
	 */
	@Override
	protected Field findField(String name, Class<?> clazz, boolean mustBeStatic) {
		// IMPORTANT: this method is copy-paste-modified from parent class.
		// Reason: get ALL fields instead of getting only PUBLIC fields (getDeclaredFields vs getFields)
		Field[] fields = clazz.getDeclaredFields();
		for (Field field : fields) {
			if (field.getName().equals(name) && (!mustBeStatic || Modifier.isStatic(field.getModifiers()))) {
				return field;
			}
		}
		return null;
	}
}