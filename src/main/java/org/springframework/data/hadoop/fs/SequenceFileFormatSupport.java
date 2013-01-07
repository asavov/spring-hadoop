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

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.springframework.data.hadoop.fs.HdfsWriter.SerializationFormat;

/**
 * The class provides support commonly needed by {@link SerializationFormat} implementations that serialize objects
 * based on their type using {@link SequenceFile} pluggable serialization framework.
 * 
 * @see {@link Serialization}
 * @see {@link SerializationFactory}
 * 
 * @author Alex Savov
 */
public abstract class SequenceFileFormatSupport extends SerializationFormatSupport {

	protected static final String HADOOP_IO_SERIALIZATIONS = "io.serializations";

	/* This property is publicly configurable. */
	protected SerializationKeyProvider serializationKeyProvider = NullSerializationKeyProvider.INSTANCE;

	/**
	 * @param serializationKey
	 */
	public void setSerializationKeyProvider(SerializationKeyProvider serializationKey) {
		this.serializationKeyProvider = serializationKey;
	}

	/**
	 * A template method writing objects to Hadoop using {@link SequenceFile} serialization.
	 * 
	 * @see {@link SequenceFile#Writer}
	 */
	protected <T> void doWrite(Iterable<? extends T> objects, Class<T> objectsClass, HdfsResource hdfsResource)
			throws IOException {

		// Delegate to Hadoop built-in SeqFile support
		SequenceFile.Writer writer = null;

		CompressionCodec codec = HdfsWriter.getHadoopCodec(getConfiguration(), getCompressionAlias());

		if (codec != null) {
			// configure BLOCK compression
			writer = SequenceFile.createWriter(getHdfsResourceLoader().getFileSystem(), getConfiguration(),
					hdfsResource.getPath(), getKeyClass(objectsClass), getValueClass(objectsClass),
					SequenceFile.CompressionType.BLOCK, codec);
		} else {
			// configure NONE compression
			writer = SequenceFile.createWriter(getHdfsResourceLoader().getFileSystem(), getConfiguration(),
					hdfsResource.getPath(), getKeyClass(objectsClass), getValueClass(objectsClass),
					SequenceFile.CompressionType.NONE);
		}

		try {
			// Loop through passed objects and write them
			for (T object : objects) {
				writer.append(getKey(object), getValue(object));
			}
		} finally {
			IOUtils.closeStream(writer);
		}
	}

	/**
	 * @return <b>.seqfile</b> is the default file extension for {@link SequenceFile} serialization.
	 */
	public String getExtension() {
		return ".seqfile";
	}

	/**
	 * Adds the {@link Serialization} scheme to the configuration, so {@link SerializationFactory} instances are aware
	 * of it.
	 * 
	 * @param serializationClass The Serialization class to register to underlying configuration.
	 */
	protected void register(@SuppressWarnings("rawtypes") Class<? extends Serialization>... serializationClasses) {

		Collection<String> serializations = getConfiguration().getStringCollection(HADOOP_IO_SERIALIZATIONS);

		for (Class<?> serializationClass : serializationClasses) {

			if (!serializations.contains(serializationClass.getName())) {

				serializations.add(serializationClass.getName());
			}
		}

		getConfiguration().setStrings(HADOOP_IO_SERIALIZATIONS, serializations.toArray(new String[serializations.size()]));
	}

	protected abstract Class<?> getKeyClass(Class<?> objectsClass);

	protected abstract Object getKey(Object object);

	protected abstract Class<?> getValueClass(Class<?> objectsClass);

	protected abstract Object getValue(Object object);

}