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

import static org.apache.hadoop.io.IOUtils.closeStream;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

/**
 * The class provides support needed by {@link SerializationFormat}s creating {@link SerializationWriter}s that
 * serialize objects based on their type using Hadoop {@link SequenceFile} pluggable serialization framework.
 * 
 * @see {@link Serialization}
 * @see {@link SerializationFactory}
 * @see {@link SequenceFileFormat}
 * @see {@link AvroSequenceFileFormat}
 * 
 * @author Alex Savov
 */
public abstract class AbstractSequenceFileFormat<T> extends SerializationFormatSupport<T> implements InitializingBean {

	protected static final String HADOOP_IO_SERIALIZATIONS = "io.serializations";

	/* The class of the objects that are serialized by this format. */
	protected final Class<T> objectsClass;

	/* This property is publicly configurable. */
	private Configuration configuration;

	/* This property is publicly configurable. */
	private SerializationKeyProvider serializationKeyProvider;

	/**
	 * @param objectsClass The class of the objects that are serialized by this serialization format.
	 */
	public AbstractSequenceFileFormat(Class<T> objectsClass) {
		this.objectsClass = objectsClass;

		serializationKeyProvider = NullWritableSerializationKeyProvider.INSTANCE;
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
	 * @param serializationKeyProvider
	 */
	public void setSerializationKeyProvider(SerializationKeyProvider serializationKeyProvider) {
		this.serializationKeyProvider = serializationKeyProvider;
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

	/**
	 * The class provides support needed by {@link SerializationWriter}s that serialize objects based on their type
	 * using Hadoop {@link SequenceFile} pluggable serialization framework.
	 * 
	 * @see {@link SequenceFile.Writer}
	 */
	protected abstract class AbstractSequenceFileWriter extends SerializationWriterSupport {

		protected final FSDataOutputStream fsOutputStream;

		/* Native SeqFile writer. */
		protected SequenceFile.Writer writer;

		protected AbstractSequenceFileWriter(OutputStream output) {

			Assert.isInstanceOf(FSDataOutputStream.class, output,
					"A FSDataOutputStream is required to write to a SeqFile.");

			this.fsOutputStream = FSDataOutputStream.class.cast(output);
		}

		@Override
		protected Closeable doOpen() throws IOException {

			// Resolve Hadoop compression based on compression alias
			CompressionCodec codec = CompressionUtils.getHadoopCompression(getConfiguration(), getCompressionAlias());

			CompressionType compressionType = codec == null ? CompressionType.NONE : CompressionType.BLOCK;

			// Delegate to Hadoop built-in SeqFile support.
			writer = SequenceFile.createWriter(getConfiguration(), fsOutputStream, getKeyClass(), getValueClass(),
					compressionType, codec);

			// Writer.close() does not close underlying stream and we need manually to close it.
			// So wrap the Writer and the OutputStream into a Closeable.
			return new Closeable() {
				@Override
				public void close() throws IOException {
					closeStream(writer);
					closeStream(fsOutputStream);
				}
			};
		}

		/**
		 * Writes objects to Hadoop using {@link SequenceFile} serialization.
		 * 
		 * @see {@link Writer#append(Object, Object)}
		 */
		@Override
		protected void doWrite(T object) throws IOException {
			writer.append(getKey(object), getValue(object));
		}

		protected abstract Class<?> getKeyClass();

		protected abstract Object getKey(T object);

		protected abstract Class<?> getValueClass();

		protected abstract Object getValue(T object);
	}

	/**
	 * The class provides support needed by {@link SerializationReader}s that deserialize objects based on their type
	 * using Hadoop {@link SequenceFile} pluggable serialization framework.
	 * 
	 * @see {@link SequenceFile.Reader}
	 */
	protected abstract class AbstractSequenceFileReader extends SerializationReaderSupport {

		// Re-used objects passed to underlying SeqFile reader.
		protected final Object KEY_TO_REUSE = null;
		protected final Object VALUE_TO_REUSE = null;

		/* Native SeqFile reader. */
		protected SequenceFile.Reader reader;

		protected final String location;

		protected AbstractSequenceFileReader(String location) {
			this.location = location;
		}

		@Override
		protected Closeable doOpen() throws IOException {
			// Open SeqFile reader to passed location.
			reader = new SequenceFile.Reader(getHdfsResourceLoader().getFileSystem(), new Path(location),
					getConfiguration());

			return reader;
		}

		/**
		 * Reads objects from Hadoop using {@link SequenceFile} serialization.
		 * 
		 * @see {@link Reader#getCurrentValue(Object)}
		 */
		@Override
		protected T doRead() throws IOException {
			// SeqFile.key is skipped. Return SeqFile.value.
			return reader.next(KEY_TO_REUSE) != null ? getValue(reader.getCurrentValue(VALUE_TO_REUSE)) : null;
		}

		/**
		 * Converts the raw serialized object to an object that is recognized (and returned) by this Reader.
		 * @param serializedObject The raw object as return by underlying SeqFile reader.
		 * @return The object recognized (and returned) by this Reader.
		 */
		protected abstract T getValue(Object serializedObject);
	}

	/**
	 * @return <b>.seqfile</b> is the default file extension for {@link SequenceFile} serialization.
	 */
	@Override
	protected String getDefaultExtension() {
		return ".seqfile";
	}

	/**
	 * Adds the {@link Serialization} scheme to the configuration, so {@link SerializationFactory} instances are aware
	 * of it.
	 * 
	 * @param serializationClass The Serialization classes to register to underlying configuration.
	 */
	@SuppressWarnings("rawtypes")
	protected void registerSeqFileSerialization(Class<? extends Serialization>... serializationClasses) {

		Configuration conf = getConfiguration();

		Collection<String> serializations = conf.getStringCollection(HADOOP_IO_SERIALIZATIONS);

		for (Class<?> serializationClass : serializationClasses) {

			if (!serializations.contains(serializationClass.getName())) {

				serializations.add(serializationClass.getName());
			}
		}

		conf.setStrings(HADOOP_IO_SERIALIZATIONS, serializations.toArray(new String[serializations.size()]));
	}

}