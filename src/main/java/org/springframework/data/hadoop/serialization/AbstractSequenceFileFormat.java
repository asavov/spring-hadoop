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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

/**
 * The class provides support needed by {@link SerializationFormat} implementations that serialize objects based on
 * their type using {@link SequenceFile} pluggable serialization framework.
 * 
 * @see {@link Serialization}
 * @see {@link SerializationFactory}
 * 
 * @author Alex Savov
 */
public abstract class AbstractSequenceFileFormat<T> extends AbstractObjectsSerializationFormat<T> implements
		InitializingBean {

	protected static final String HADOOP_IO_SERIALIZATIONS = "io.serializations";

	/* This property is publicly configurable. */
	private Configuration configuration;

	/* This property is publicly configurable. */
	private SerializationKeyProvider serializationKeyProvider;

	/* Native SeqFile writer. */
	private SequenceFile.Writer writer;

	protected AbstractSequenceFileFormat(Class<T> objectsClass) {
		super(objectsClass);

		serializationKeyProvider = NullSerializationKeyProvider.INSTANCE;
	}

	/**
	 * Sets the Hadoop configuration for this <code>SerializationFormat</code>.
	 * 
	 * @param configuration The configuration to use.
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

	protected Configuration getConfiguration() {
		return configuration;
	}

	/**
	 * @param serializationKey
	 */
	public void setSerializationKeyProvider(SerializationKeyProvider serializationKey) {
		this.serializationKeyProvider = serializationKey;
	}

	protected SerializationKeyProvider getSerializationKeyProvider() {
		return serializationKeyProvider;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		// TODO: @Costin: Should we clone passed Configuration or should we use it as it is?
		// My take is to clone it cause it's changed by 'register' method. Or is that a responsibility of the caller?
		Assert.notNull(getConfiguration(), "A non-null Hadoop configuration is required.");
		Assert.notNull(getSerializationKeyProvider(), "A non-null SerializationKeyProvider is required.");
	}

	@Override
	protected Closeable doOpen() throws IOException {

		Assert.isInstanceOf(FSDataOutputStream.class, getOutputStream(),
				"A FSDataOutputStream is required to write to a SeqFile.");

		CompressionCodec codec = CompressionUtils.getHadoopCompression(getConfiguration(), getCompressionAlias());

		CompressionType compressionType = codec == null ? CompressionType.NONE : CompressionType.BLOCK;

		// Delegate to Hadoop built-in SeqFile support
		writer = SequenceFile.createWriter(getConfiguration(), FSDataOutputStream.class.cast(getOutputStream()),
				getKeyClass(objectsClass), getValueClass(objectsClass), compressionType, codec);

		// Writer.close() does not close underlying stream and we need manually to close it.
		// So wrap the Writer and the OutputStream into a Closeable.
		return new Closeable() {
			@Override
			public void close() throws IOException {
				IOUtils.closeStream(writer);
				IOUtils.closeStream(getOutputStream());
			}
		};
	}

	/**
	 * Writes objects to Hadoop using {@link SequenceFile} serialization.
	 * 
	 * @see {@link SequenceFile#Writer}
	 */
	@Override
	protected void doSerialize(Iterable<? extends T> objects) throws IOException {
		// Loop through passed objects and write them
		for (T object : objects) {
			writer.append(getKey(object), getValue(object));
		}
	}

	/**
	 * @return <b>.seqfile</b> is the default file extension for {@link SequenceFile} serialization.
	 */
	@Override
	public String getExtension() {
		return ".seqfile";
	}

	protected abstract Class<?> getKeyClass(Class<?> objectsClass);

	protected abstract Object getKey(Object object);

	protected abstract Class<?> getValueClass(Class<?> objectsClass);

	protected abstract Object getValue(Object object);

	/**
	 * Adds the {@link Serialization} scheme to the configuration, so {@link SerializationFactory} instances are aware
	 * of it.
	 * 
	 * @param serializationClass The Serialization class to register to underlying configuration.
	 */
	@SuppressWarnings("rawtypes")
	protected static void registerSeqFileSerialization(Configuration conf,
			Class<? extends Serialization>... serializationClasses) {

		Collection<String> serializations = conf.getStringCollection(HADOOP_IO_SERIALIZATIONS);

		for (Class<?> serializationClass : serializationClasses) {

			if (!serializations.contains(serializationClass.getName())) {

				serializations.add(serializationClass.getName());
			}
		}

		conf.setStrings(HADOOP_IO_SERIALIZATIONS, serializations.toArray(new String[serializations.size()]));
	}

}