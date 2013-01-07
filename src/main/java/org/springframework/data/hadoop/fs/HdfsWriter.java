/*
 * Copyright 2012 the original author or authors.
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
package org.springframework.data.hadoop.fs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collection;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.serializer.JavaSerialization;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.util.ReflectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.MethodParameter;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.HadoopException;
import org.springframework.expression.AccessException;
import org.springframework.expression.spel.support.ReflectivePropertyAccessor;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * Utility class providing 'write to HDFS' functionality. It is supposed to be used outside of Hadoop to move data into
 * HDFS. Features supported:
 * <ul>
 * <li>Write resource: writes the content of the {@link Resource} source to provided HDFS destination.</li>
 * <li>Write collection of Java objects: writes a collection of Java objects to provided HDFS destination. A
 * serialization format could be additionally specified to customize objects persistence.</li>
 * <li>Write with compression: same as above plus compress output data on the fly.</li>
 * </ul>
 * 
 * @author Alex Savov
 */
public class HdfsWriter {

	/**
	 * This interface represents a strategy to write a collection of Java objects to HDFS. See specific implementations
	 * for more details.
	 */
	// @Costin: Enum as inner or separate class? The same question applies for other inner classes/interfaces.
	// NOTE [naming]: SerializationFormat vs SerializationWriter?
	public static interface SerializationFormat {

		// @Costin: should we "mirror" Hadoop SequenceFile serialization framework to some extend?
		// boolean accept(Class<?> objectsClass);

		/**
		 * Mirrors core {@link HdfsWriter#write(Collection, Class, String)} method encapsulating just a single
		 * serialization use-case, such as write to SequenceFiles using Java serialization.
		 * 
		 * @param objects The objects to write.
		 * @param objectsClass The class of objects to write.
		 * @param destination HDFS destination file path to write to.
		 * 
		 * @throws IOException
		 */
		<T> void write(Iterable<? extends T> objects, Class<T> objectsClass, String destination) throws IOException;

	}

	/**
	 * An instance of this interface is responsible to provide a key for an object that is written to HDFS using a
	 * serialization format which stores the data as key-value pairs. The object is stored as a value while the key is
	 * provided by this interface.
	 */
	// @Costin: Should we somehow utilize ConversionService/[Generic]Converter or conform to its design?
	// Should we go with generic types? Any Comments?
	public static interface SerializationKeyProvider {

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

	@Autowired
	private FileSystem fs;

	@Autowired
	private Configuration config;

	@Autowired
	private HdfsResourceLoader hdfsResourceLoader;

	/* The property is publicly configurable. */
	private String codecAlias;

	/* The property is publicly configurable. */
	private SerializationFormat serializationFormat = new SequenceFileWriter();

	/* This property is publicly configurable. */
	private SerializationKeyProvider serializationKeyProvider = NullSerializationKeyProvider.INSTANCE;

	/**
	 * @return the serializationKey
	 */
	public SerializationKeyProvider getSerializationKeyProvider() {
		return serializationKeyProvider;
	}

	/**
	 * @param serializationKey
	 */
	public void setSerializationKeyProvider(SerializationKeyProvider serializationKey) {
		this.serializationKeyProvider = serializationKey;
	}

	/**
	 * @return the serialization
	 */
	public SerializationFormat getSerializationFormat() {
		return serializationFormat;
	}

	/**
	 * @param serialization
	 */
	public void setSerializationFormat(SerializationFormat serialization) {
		this.serializationFormat = serialization;
	}

	/**
	 * @return the codecAlias
	 */
	public String getCodecAlias() {
		return codecAlias;
	}

	/**
	 * @param codecAlias Accepted values:
	 * <ul>
	 * <li>The short class name (without the package) of the compression codec that is specified within Hadoop
	 * configuration (under <i>io.compression.codecs</i> prop). If the short class name ends with 'Codec', then there
	 * are two aliases for the codec - the complete short class name and the short class name without the 'Codec'
	 * ending. For example for the 'GzipCodec' codec class name the alias are 'gzip' and 'gzipcodec' (case insensitive).
	 * If the codec is configured to be used by Hadoop this is the preferred way instead of passing the codec canonical
	 * name.</li>
	 * <li>The canonical class name of the compression codec that is specified within Hadoop configuration (under
	 * <i>io.compression.codecs</i> prop) or is present on the classpath.</li>
	 * </ul>
	 */
	public void setCodecAlias(String codecAlias) {
		this.codecAlias = codecAlias;
	}

	/**
	 * Writes the content of source resource to the destination.
	 * 
	 * @param source The source to read from.
	 * @param destination HDFS destination file path to write to.
	 */
	public void write(Resource source, String destination) {

		CompressionCodec codec = getCodec();

		if (codec != null) {
			if (!destination.toLowerCase().endsWith(codec.getDefaultExtension().toLowerCase())) {
				destination += codec.getDefaultExtension();
			}
		}

		InputStream inputStream = null;
		OutputStream outputStream = null;
		try {
			inputStream = source.getInputStream();

			HdfsResource hdfsResource = (HdfsResource) hdfsResourceLoader.getResource(destination);

			// Open new HDFS file
			outputStream = hdfsResource.getOutputStream();

			// Apply compression
			if (codec != null) {
				outputStream = codec.createOutputStream(outputStream);
				// TODO: Eventually re-use underlying Compressor through CodecPool.
			}

			// Write source to HDFS destination
			IOUtils.copyBytes(inputStream, outputStream, config, /* close */false);

		} catch (IOException ioExc) {

			throw new HadoopException("Cannot write resource: " + ioExc.getMessage(), ioExc);

		} finally {
			IOUtils.closeStream(outputStream);
			IOUtils.closeStream(inputStream);
		}
	}

	/**
	 * Writes a collection of Java object to HDFS at provided destination.
	 * 
	 * Objects serialization is delegated to configured {@link SerializationFormat}.
	 * 
	 * @param objects The objects to write.
	 * @param objectsClass The class of objects to write.
	 * @param destination HDFS destination file path to write to.
	 */
	// @Costin: Collection vs. Iterable? Any preference?
	public <T> void write(Iterable<? extends T> objects, Class<T> objectsClass, String destination) {

		try {
			// Delegate to core 'write objects' logic
			getSerializationFormat().write(objects, objectsClass, destination);

		} catch (IOException ioExc) {
			throw new HadoopException("Cannot write objects: " + ioExc.getMessage(), ioExc);
		}
	}

	/**
	 * The class provides support commonly needed by {@link SerializationFormat} implementations. This includes:
	 * <ul>
	 * <li>Resolving of passed <code>destination</code> to HDFS {@link Resource}.</li>
	 * <li>Exposing hook methods to allow 'write of objects' customization.</li>
	 * </ul>
	 */
	// @Costin: TODO [IMPORTANT] how to pass HdfsWrite context (codec, hdfs loader, etc) to descendant classes
	// and make them self-contained and decoupled from HdfsWrite parent class.
	// TODO: Should we clone passed Configuration or should we use it as it is?
	public abstract class SerializationFormatSupport extends Configured implements SerializationFormat {

		/**
		 * A template method writing objects to HDFS. Provides hook methods subclasses should override to encapsulate
		 * their specific logic.
		 */
		public <T> void write(Iterable<? extends T> objects, Class<T> objectsClass, String destination)
				throws IOException {

			// Resolve passed destination to HDFS Resource
			HdfsResource hdfsResource;
			{
				if (!destination.toLowerCase().endsWith(getExtension().toLowerCase())) {
					destination += getExtension();
				}

				hdfsResource = (HdfsResource) hdfsResourceLoader.getResource(destination);
			}

			// Configuration step
			doInit(objects, objectsClass, hdfsResource);

			// Core write step
			doWrite(objects, objectsClass, hdfsResource);
		}

		/**
		 * A hook method for executing configuration logic prior objects write.
		 * 
		 * @param objects
		 * @param objectsClass
		 * @param hdfsResource
		 */
		protected abstract <T> void doInit(Iterable<? extends T> objects, Class<T> objectsClass,
				HdfsResource hdfsResource);

		/**
		 * Do the core write logic.
		 * 
		 * @param objects
		 * @param objectsClass
		 * @param hdfsResource
		 * @throws IOException
		 */
		protected abstract <T> void doWrite(Iterable<? extends T> objects, Class<T> objectsClass,
				HdfsResource hdfsResource) throws IOException;

		/**
		 * Gets the filename extension for this kind of serialization format (such as '.avro' or '.seqfile').
		 * 
		 * @return The file extension including the '.' char.
		 */
		protected abstract String getExtension();

	}

	/**
	 * An abstract writer that serializes objects based on their type using {@link SequenceFile} pluggable serialization
	 * framework.
	 * 
	 * @see {@link Serialization}
	 * @see {@link SerializationFactory}
	 */
	public abstract class AbstractSequenceFileWriter extends SerializationFormatSupport {

		/**
		 * A template method writing objects to Hadoop using {@link SequenceFile} serialization.
		 * 
		 * @see {@link SequenceFile#Writer}
		 */
		protected <T> void doWrite(Iterable<? extends T> objects, Class<T> objectsClass, HdfsResource hdfsResource)
				throws IOException {

			CompressionCodec codec = getCodec();

			// Delegate to Hadoop built-in SeqFile support
			SequenceFile.Writer writer = null;

			if (codec != null) {
				// configure BLOCK compression
				writer = SequenceFile.createWriter(fs, getConf(), hdfsResource.getPath(), getKeyClass(objectsClass),
						getValueClass(objectsClass), SequenceFile.CompressionType.BLOCK, codec);
			} else {
				// configure NONE compression
				writer = SequenceFile.createWriter(fs, getConf(), hdfsResource.getPath(), getKeyClass(objectsClass),
						getValueClass(objectsClass), SequenceFile.CompressionType.NONE);
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
		protected String getExtension() {
			return ".seqfile";
		}

		/**
		 * Adds the {@link Serialization} scheme to the configuration, so {@link SerializationFactory} instances are
		 * aware of it.
		 * 
		 * @param serializationClass The Serialization class to register to underlying configuration.
		 */
		protected void register(@SuppressWarnings("rawtypes") Class<? extends Serialization>... serializationClasses) {

			final String HADOOP_IO_SERIALIZATIONS = "io.serializations";

			Configuration conf = getConf();

			Collection<String> serializations = conf.getStringCollection(HADOOP_IO_SERIALIZATIONS);

			for (Class<?> serializationClass : serializationClasses) {

				if (!serializations.contains(serializationClass.getName())) {

					serializations.add(serializationClass.getName());
				}
			}

			conf.setStrings(HADOOP_IO_SERIALIZATIONS, serializations.toArray(new String[serializations.size()]));
		}

		protected abstract Class<?> getKeyClass(Class<?> objectsClass);

		protected abstract Object getKey(Object object);

		protected abstract Class<?> getValueClass(Class<?> objectsClass);

		protected abstract Object getValue(Object object);

	}

	/**
	 * Serialization format for writing Java classes that are accepted by a {@link Serialization} registered with the
	 * {@link SerializationFactory} using Hadoop {@link SequenceFile} serialization framework. By default Hadoop comes
	 * with serialization support for {@link Serializable} and {@link Writable} classes.
	 */
	public class SequenceFileWriter extends AbstractSequenceFileWriter {

		/**
		 * Adds <code>WritableSerialization</code> and <code>JavaSerialization</code> schemes to Hadoop configuration,
		 * so {@link SerializationFactory} instances constructed from the given configuration will be aware of it.
		 */
		@SuppressWarnings("unchecked")
		protected <T> void doInit(Iterable<? extends T> objects, Class<T> objectsClass, HdfsResource hdfsResource) {

			register(WritableSerialization.class, JavaSerialization.class);
		}

		protected Class<?> getKeyClass(Class<?> objectClass) {
			return getSerializationKeyProvider().getKeyClass(objectClass);
		}

		protected Object getKey(Object object) {
			return getSerializationKeyProvider().getKey(object);
		}

		protected Class<?> getValueClass(Class<?> objectClass) {
			return objectClass;
		}

		protected Object getValue(Object object) {
			return object;
		}
	}

	/**
	 * Serialization format for writing POJOs in <code>Avro</code> schema using Hadoop {@link SequenceFile}
	 * serialization framework.
	 */
	public class AvroSequenceFileWriter extends AbstractSequenceFileWriter {

		@Override
		@SuppressWarnings("unchecked")
		protected <T> void doInit(Iterable<? extends T> objects, Class<T> objectsClass, HdfsResource hdfsResource) {

			// Reflective Avro schema of key class
			Schema keySchema = ReflectData.get().getSchema(getSerializationKeyProvider().getKeyClass(objectsClass));

			AvroSerialization.setKeyWriterSchema(getConf(), keySchema);

			// Reflective Avro schema of value class
			Schema valueSchema = ReflectData.get().getSchema(objectsClass);

			AvroSerialization.setValueWriterSchema(getConf(), valueSchema);

			register(AvroSerialization.class);
		}

		/**
		 * @return <code>AvroKey.class</code> is always used to write Avro data
		 */
		protected final Class<?> getKeyClass(Class<?> objectClass) {
			return AvroKey.class;
		}

		/**
		 * @return new <code>AvroKey</code> wrapper around core object key
		 */
		@SuppressWarnings({ "unchecked", "rawtypes" })
		protected Object getKey(Object object) {
			return new AvroKey(getSerializationKeyProvider().getKey(object));
		}

		/**
		 * @return <code>AvroValue.class</code> is always used to write Avro data
		 */
		protected final Class<?> getValueClass(Class<?> objectClass) {
			return AvroValue.class;
		}

		/**
		 * @return new <code>AvroValue</code> wrapper around core object
		 */
		@SuppressWarnings({ "unchecked", "rawtypes" })
		protected Object getValue(Object object) {
			return new AvroValue(object);
		}
	}

	/**
	 * Serialization format for writing POJOs using <code>Avro</code> serialization. File extension: <i>.avro<i>.
	 */
	public class AvroWriter extends SerializationFormatSupport {

		protected <T> void doWrite(Iterable<? extends T> objects, Class<T> objectsClass, HdfsResource hdfsResource)
				throws IOException {

			DataFileWriter<T> writer = null;
			OutputStream outputStream = null;
			try {
				Schema schema = ReflectData.get().getSchema(objectsClass);

				writer = new DataFileWriter<T>(new ReflectDatumWriter<T>(schema));

				writer.setCodec(getAvroCodec());

				outputStream = hdfsResource.getOutputStream();

				writer.create(schema, outputStream);

				for (T object : objects) {
					writer.append(object);
				}
			} finally {
				// The order is VERY important.
				IOUtils.closeStream(writer);
				IOUtils.closeStream(outputStream);
			}
		}

		protected <T> void doInit(Iterable<? extends T> objects, Class<T> objectsClass, HdfsResource hdfsResource) {
			// do nothing
		}

		/**
		 * @return <b>.avro</b> is the default file extension for {@link SequenceFile} serialization.
		 */
		protected String getExtension() {
			return ".avro";
		}

		// TODO: We need a story how to unify/abstract Hadoop and Avro codecs.
		private CodecFactory getAvroCodec() {
			return StringUtils.hasText(codecAlias) ? CodecFactory.fromString(codecAlias) : CodecFactory.nullCodec();
		}
	}

	/**
	 * @return The codec to be used to compress the data on the fly while storing it onto HDFS, if the
	 * <code>codecAlias</code> property is specified; <code>null</code> otherwise.
	 */
	private CompressionCodec getCodec() {

		if (!StringUtils.hasText(codecAlias)) {
			return null;
		}

		final CompressionCodecFactory codecFactory = new CompressionCodecFactory(config);

		// Find codec by canonical class name or by codec alias as specified in Hadoop configuration
		// @Costin: TODO: The method is NOT available in hadoop-core.1.0.4.jar, but in CDH3 version. How to h it?
		CompressionCodec codec = codecFactory.getCodecByName(codecAlias);

		// If the codec is not configured within Hadoop try to load it from the classpath
		if (codec == null) {
			Class<?> codecClass = ClassUtils.resolveClassName(codecAlias, getClass().getClassLoader());

			// Instantiate codec and initialize it from configuration
			// org.apache.hadoop.util.ReflectionUtils design is specific to Hadoop env :)
			codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, config);
		}

		// TODO: Should we fall back to some default codec if not resolved?
		return codec;
	}

	/**
	 * Default implementation of {@link SerializationKeyProvider} returning {@link NullWritable} key for every object.
	 */
	public static class NullSerializationKeyProvider implements SerializationKeyProvider {

		public static final NullSerializationKeyProvider INSTANCE = new NullSerializationKeyProvider();

		/**
		 * @return The singleton <code>NullWritable</code> as returned by {@link NullWritable#get()} method.
		 */
		public NullWritable getKey(Object object) {
			return NullWritable.get();
		}

		/**
		 * @return <code>NullWritable.class</code> for every object class
		 */
		public Class<NullWritable> getKeyClass(Class<?> objectClass) {
			return NullWritable.class;
		}
	}

	/**
	 * Reflective implementation of {@link SerializationKeyProvider} returning specific object property as object key. A
	 * property is accessible through a getter method or a field of the object.
	 */
	// @Costin: Is there a class that provides reflective info (TypeDescriptor) and property access based on Class.
	// Both beans.PropertyAccessor and expression.PropertyAccessor interfaces provide metadata only by object instance.
	public static class ReflectiveSerializationKeyProvider extends ReflectivePropertyAccessor implements
			SerializationKeyProvider {

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

			// @Costin: Unfortunately [Reflective]PropertyAccessor does not provide access to underlying TypeDescriptor,
			// so we need manually to extra it and mimic [can]Read method. Is there a better way to achieve that?

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
		 * @return The class of the key property (either getter method or field). <code>null</code> if passed object
		 * class is not supported by this provider.
		 */
		public Class<?> getKeyClass(Class<?> objectClass) {
			return (this.objectClass == objectClass) ? keyDescriptor.getObjectType() : null;
		}

		/**
		 * Find a declared field (as specified by {@link Class#getDeclaredField(String)} contract) of a certain name on
		 * a specified class.
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

}
