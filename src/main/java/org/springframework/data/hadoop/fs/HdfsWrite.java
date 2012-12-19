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
import java.util.Collection;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.serializer.JavaSerialization;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.util.ReflectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * Utility class providing 'write into HDFS' functionality. It's supposed to be used outside of Hadoop to move data into
 * HDFS. Features supported:
 * <ul>
 * <li>Write file: pass a source and HDFS destination and it writes the file into HDFS.</li>
 * <li>Write file with compression: pass a source, HDFS destination and a compression and it writes the file into HDFS
 * compressing its content on the fly.</li>
 * </ul>
 * 
 * @author Alex Savov
 */
// TODO: Update JavaDoc once we agree upon on the design.
public class HdfsWrite {

	public static enum SerializationType {

		AVRO(".avro"), JAVA(".seqfile"), WRITABLE(".seqfile");

		private final String extension;

		private SerializationType(String extension) {
			this.extension = extension;
		}

		public String getExtension() {
			return extension;
		}
	}

	@Autowired
	private FileSystem fs;

	@Autowired
	private Configuration config;

	@Autowired
	private HdfsResourceLoader hdfsResourceLoader;

	private String codecAlias;

	private SerializationType serialization = SerializationType.JAVA;

	/**
	 * @return the serialization
	 */
	public SerializationType getSerialization() {
		return serialization;
	}

	/**
	 * @param serialization the serialization to set
	 */
	public void setSerialization(SerializationType serialization) {
		this.serialization = serialization;
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
	 * @param destination The destination to write to.
	 * 
	 * @throws IOException
	 */
	public void write(Resource source, String destination) throws IOException {

		CompressionCodec codec = getHadoopCodec();

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
			IOUtils.copyBytes(inputStream, outputStream, config, /*close*/false);
		} finally {
			IOUtils.closeStream(outputStream);
			IOUtils.closeStream(inputStream);
		}
	}

	/**
	 * Writes Java object to a HDFS sequence file at provided destination.
	 * 
	 * The object is serialized based on its type by SequenceFiles pluggable serialization framework. Hadoop comes with
	 * four serializers: Avro, Java, Tether and Writable (the default serializer).
	 * 
	 * @param object
	 * @param destination
	 * @throws IOException
	 * 
	 * @see {@link Serialization}
	 * @see {@link SerializationFactory}
	 */
	public <T> void write(Collection<? extends T> objects, Class<T> objectsClass, String destination)
			throws IOException {

		if (!destination.toLowerCase().endsWith(getSerialization().getExtension().toLowerCase())) {
			destination += getSerialization().getExtension();
		}

		HdfsResource hdfsResource = (HdfsResource) hdfsResourceLoader.getResource(destination);

		if (serialization == SerializationType.AVRO) {
			writeAvro(objects, objectsClass, hdfsResource);
		} else {
			writeSeqFile(objects, objectsClass, hdfsResource);
		}
	}

	protected <T> void writeSeqFile(Collection<? extends T> objects, Class<T> objectsClass, HdfsResource hdfsResource)
			throws IOException {

		initSerializations();

		CompressionCodec codec = getHadoopCodec();

		SequenceFile.Writer writer = null;

		if (codec != null) {
			writer = SequenceFile.createWriter(fs, config, hdfsResource.getPath(), NullWritable.class, objectsClass,
					SequenceFile.CompressionType.BLOCK, codec);
		} else {
			writer = SequenceFile.createWriter(fs, config, hdfsResource.getPath(), NullWritable.class, objectsClass,
					SequenceFile.CompressionType.NONE);
		}

		try {
			for (T object : objects) {
				writer.append(NullWritable.get(), object);
			}
		} finally {
			IOUtils.closeStream(writer);
		}
	}

	protected <T> void writeAvro(Collection<? extends T> objects, Class<T> objectsClass, HdfsResource hdfsResource)
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

	/**
	 * @return The codec to be used to compress the data on the fly while storing it onto HDFS if the
	 * <code>codecAlias</code> property is specified; <code>null</code> otherwise.
	 */
	private CompressionCodec getHadoopCodec() {

		if (!StringUtils.hasText(codecAlias)) {
			return null;
		}

		final CompressionCodecFactory codecFactory = new CompressionCodecFactory(config);

		// Find codec by canonical class name or by codec alias as specified in Hadoop
		// configuration
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

	private CodecFactory getAvroCodec() {
		return StringUtils.hasText(codecAlias) ? CodecFactory.fromString(codecAlias) : CodecFactory.nullCodec();
	}

	/**
	 * Configure Hadoop to support WritableSerialization and JavaSerialization in addition to present ones.
	 * 
	 * NOTE: Only WritableSerialization is supported by default.
	 */
	private void initSerializations() {

		final String HADOOP_IO_SERIALIZATIONS = "io.serializations";

		String[] existing = (String[]) config.getStrings(HADOOP_IO_SERIALIZATIONS);

		String[] serializations = { WritableSerialization.class.getCanonicalName(),
				JavaSerialization.class.getCanonicalName() };

		String[] merged = StringUtils.mergeStringArrays(existing, serializations);

		config.set(HADOOP_IO_SERIALIZATIONS, StringUtils.arrayToCommaDelimitedString(merged));
	}

}
