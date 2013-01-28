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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.fs.HdfsResource;
import org.springframework.expression.AccessException;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.ClassUtils;

/**
 * Integration test for {@link SerializationFormatFactoryBean} testing simple and compressed writes of a file to HDFS.
 * 
 * @author Alex Savov
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class HdfsWriterTest {

	@Autowired
	private SerializationFormatFactoryBean sfFactoryBean;

	@Autowired
	private Configuration configuration;

	/**
	 * The file that's written to HDFS with[out] compression.
	 */
	@Value("classpath:/data/apache-short.txt")
	private Resource sourceResource;

	/**
	 * All output files are written to that HDFS dir.
	 */
	@Value("${hdfs.writer.output.dir}")
	private String hdfsOutputDir;

	private ResourceSerializationFormatCreator RESOURCE;

	private AvroFormatCreator<PojoSerializable> AVRO;

	private SequenceFileFormatCreator<PojoSerializable> SEQUENCE_FILE_JAVA;

	private SequenceFileFormatCreator<PojoWritable> SEQUENCE_FILE_WRITABLE;

	private AvroSequenceFileFormatCreator<PojoSerializable> SEQUENCE_FILE_AVRO;

	@Before
	public void initSerializationFormatFactories() throws Exception {

		RESOURCE = new ResourceSerializationFormatCreator();
		RESOURCE.setConfiguration(configuration);
		RESOURCE.afterPropertiesSet();

		// TODO: If we decide to clone Configuration for every SerializationFormat then this code is required.
		/*
		 * Collection<String> serializations = hdfs.configuration.getStringCollection(HADOOP_IO_SERIALIZATIONS);
		 * 
		 * Class<?>[] sClasses = { WritableSerialization.class, JavaSerialization.class, AvroSerialization.class };
		 * 
		 * for (Class<?> serializationClass : sClasses) {
		 * 
		 * if (!serializations.contains(serializationClass.getName())) {
		 * 
		 * serializations.add(serializationClass.getName()); } }
		 * 
		 * hdfs.configuration.setStrings(HADOOP_IO_SERIALIZATIONS, serializations.toArray(new
		 * String[serializations.size()]));
		 */

		// NOTE: So far we share the same Configuration between SerializationFormats.

		SEQUENCE_FILE_WRITABLE = new SequenceFileFormatCreator<PojoWritable>(PojoWritable.class);
		SEQUENCE_FILE_WRITABLE.setConfiguration(configuration);
		SEQUENCE_FILE_WRITABLE.afterPropertiesSet();

		SEQUENCE_FILE_JAVA = new SequenceFileFormatCreator<PojoSerializable>(PojoSerializable.class);
		SEQUENCE_FILE_JAVA.setConfiguration(configuration);
		SEQUENCE_FILE_JAVA.afterPropertiesSet();

		SEQUENCE_FILE_AVRO = new AvroSequenceFileFormatCreator<PojoSerializable>(PojoSerializable.class);
		SEQUENCE_FILE_AVRO.setConfiguration(configuration);
		SEQUENCE_FILE_AVRO.afterPropertiesSet();

		AVRO = new AvroFormatCreator<PojoSerializable>(PojoSerializable.class);
	}

	/*
	 * Test write from source file to HDFS destination.
	 */
	@Test
	public void testWriteSimple() throws Exception {

		testResourceWrite(/* codec */null, /* doesnt matter */false);
	}

	/**
	 * Test write of pojos collection using Writable serialization.
	 */
	@Test
	public void testWriteOfWritableToSeqFile() throws Exception {

		testSerializationWrite(PojoWritable.class, SEQUENCE_FILE_WRITABLE, /* compress */false);
	}

	/**
	 * Test compression write of pojos collection using Writable serialization.
	 */
	@Test
	public void testCompressedWriteOfWritableToSeqFile() throws Exception {

		testSerializationWrite(PojoWritable.class, SEQUENCE_FILE_WRITABLE, /* compress */true);
	}

	/**
	 * Test write of pojos collection using Java serialization.
	 */
	@Test
	public void testWriteOfSerializableToSeqFile() throws Exception {

		testSerializationWrite(PojoSerializable.class, SEQUENCE_FILE_JAVA, /* compress */false);
	}

	/**
	 * Test compressed write of pojos collection using Java serialization.
	 */
	@Test
	public void testCompressedWriteOfSerializableToSeqFile() throws Exception {

		testSerializationWrite(PojoSerializable.class, SEQUENCE_FILE_JAVA, /* compress */true);
	}

	/**
	 * Test write of pojos collection using Avro serialization.
	 */
	@Test
	public void testWriteOfAvroToSeqFile() throws Exception {

		SEQUENCE_FILE_AVRO.setSerializationKeyProvider(new SerializationKeyProvider() {

			public Void getKey(Object object) {
				return (Void) null;
			}

			public Class<Void> getKeyClass(Class<?> objectClass) {
				return Void.class;
			}
		});

		testSerializationWrite(PojoSerializable.class, SEQUENCE_FILE_AVRO, /* compress */false);
	}

	/**
	 * Test compressed write of pojos collection using Avro serialization.
	 */
	@Test
	public void testCompressedWriteOfAvroToSeqFile() throws Exception {

		testSerializationWrite(PojoSerializable.class, SEQUENCE_FILE_AVRO, /* compress */true);
	}

	/**
	 * Test write of pojos collection using Avro serialization.
	 */
	@Test
	public void testWriteOfPojoToAvroFile() throws Exception {

		testSerializationWrite(PojoSerializable.class, AVRO, /* compress */false);
	}

	/**
	 * Test compression write of pojos collection using Avro serialization.
	 */
	@Test
	public void testCompressedtestWriteOfPojoToAvroFile() throws Exception {

		testSerializationWrite(PojoSerializable.class, AVRO, /* compress */true);
	}

	/**
	 * Test compressed write from source file to HDFS destination using codec alias as configured within Hadoop.
	 */
	@Test
	public void testCompressedWriteUsingHadoopCodecAlias() throws Exception {

		// DefaultCodec is configured by Hadoop by default
		final CompressionCodec codec = new CompressionCodecFactory(configuration).getCodecByName(DefaultCodec.class
				.getSimpleName());

		testResourceWrite(codec, /* useCodecAlias */true);
	}

	/**
	 * Test compressed write from source file to HDFS destination using codec class name as configured within Hadoop.
	 */
	@Test
	public void testCompressedWriteUsingHadoopCodecClassName() throws Exception {

		// GzipCodec is configured by Hadoop by default
		final CompressionCodec codec = new CompressionCodecFactory(configuration).getCodecByName(GzipCodec.class
				.getSimpleName());

		testResourceWrite(codec, /* useCodecAlias */false);
	}

	/**
	 * Test compressed write from source file to HDFS destination using user provided codec loaded from the classpath.
	 */
	@Test
	public void testCompressedWriteUsingUserCodecClassName() throws Exception {

		// CustomCompressionCodec is NOT supported by Hadoop, but is provided by the
		// client on the classpath
		final CompressionCodec codec = new CustomCompressionCodec();

		testResourceWrite(codec, /* useCodecAlias */false);
	}

	/**
	 * Test compressed write of source file against ALL codecs supported by Hadoop.
	 */
	@Test
	public void testCompressedWriteUsingHadoopCodecs() {
		// Might be re-worked to support parameterized tests.
		// See @Parameterized and Parameterized.Parameters

		hdfsOutputDir += "hadoop-codecs/";

		final StringBuilder exceptions = new StringBuilder();

		// Get a list of all codecs supported by Hadoop
		for (Class<? extends CompressionCodec> codecClass : CompressionCodecFactory.getCodecClasses(configuration)) {
			try {
				testResourceWrite(ReflectionUtils.newInstance(codecClass, configuration), /* useCodecAlias */true);
			} catch (Exception exc) {
				exceptions.append(codecClass.getName() + " not supported. Details: " + exc.getMessage() + "\n");
			}
		}

		assertTrue(exceptions.toString(), exceptions.length() == 0);
	}

	/**
	 * Test {@link ReflectiveSerializationKeyProvider}.
	 */
	@Test
	public void testReflectiveSerializationKeyProvider() throws AccessException {

		// Test field key
		{
			SerializationKeyProvider keyProvider = new ReflectiveSerializationKeyProvider(PojoSerializable.class, "id");

			assertSame(Integer.class, keyProvider.getKeyClass(PojoSerializable.class));

			PojoSerializable pojo = new PojoSerializable();

			assertEquals((Integer) pojo.id, keyProvider.getKey(pojo));
		}

		// Test getter key
		{
			SerializationKeyProvider keyProvider = new ReflectiveSerializationKeyProvider(PojoSerializable.class,
					"name");

			assertSame(String.class, keyProvider.getKeyClass(PojoSerializable.class));

			PojoSerializable pojo = new PojoSerializable();

			assertEquals(pojo.getName(), keyProvider.getKey(pojo));
		}
	}

	/**
	 * Test core Resource [compressed] write logic.
	 * 
	 * @param codec Used ONLY to get codec extension and its class name or alias in a type-safe manner.
	 * @param useAlias If <code>true</code> uses <code>codec.getClass().getSimpleName()</code> as a codec alias.
	 * Otherwise uses <code>codec.getClass().getName()</code> as a codec class name.
	 */
	private void testResourceWrite(CompressionCodec codec, boolean useAlias) throws Exception {

		if (codec != null) {
			// configure compression
			RESOURCE.setCompressionAlias(useAlias ? codec.getClass().getSimpleName() : codec.getClass()
					.getName());
		}

		// calculates the destination for Resource source.
		String destination;
		{
			destination = hdfsOutputDir;

			// add file name
			destination += sourceResource.getFilename();
			// add serialization format name
			destination += "_" + RESOURCE.getClass().getSimpleName();
		}

		hdfsWrite(RESOURCE, Collections.singleton(sourceResource), destination);

		// expected destination on hdfs should have codec extension appended
		assertHdfsFileExists(destination + RESOURCE.getExtension());
	}

	/**
	 * Test core write-of-objects logic.
	 */
	private <T> void testSerializationWrite(Class<T> objectClass, SerializationFormatCreatorSupport<?> serializationFactory,
			boolean compress) throws Exception {

		String destination;
		{
			destination = hdfsOutputDir;

			// add class name
			destination += objectClass.getSimpleName();
			// add serialization format name
			destination += "_" + serializationFactory.getClass().getSimpleName();
			// add compression flag
			destination += (compress ? "_compressed" : "");
			// add serialization format extension
			destination += serializationFactory.getExtension();
		}

		if (compress) {
			// Use default Hadoop compression (via its alias) also supported by Avro!
			serializationFactory.setCompressionAlias("deflate");
		}

		List<T> objects = createPojoList(objectClass, 5000);
		
		hdfsWrite((SerializationFormatCreator<T>) serializationFactory, objects, destination);

		assertHdfsFileExists(destination);

		List<?> readObjects = (serializationFactory == AVRO) ? readFromAvro(destination) : readFromSeqFile(destination);

		assertEquals(objects, readObjects);
	}

	@SuppressWarnings("unchecked")
	private <T> void hdfsWrite(SerializationFormatCreator<T> serializationFactory, Iterable<T> source, String destination) throws Exception {

		// Delegate to core SerializationFormat logic.
		sfFactoryBean.setDestination(destination);			
		sfFactoryBean.setSerializationFormatCreator(serializationFactory);
		
		SerializationFormat<T> serializationFormat = null;
		try {			
			// it's safe to cast since passed SerializationFormatFactory<T> returns exactly SerializationFormat<T>
			serializationFormat = (SerializationFormat<T>) sfFactoryBean.getObject();
			
			for (T aSource : source) {
				serializationFormat.serialize(aSource);				
			}
		} finally {
			IOUtils.closeStream(serializationFormat);
		}
	}

	@SuppressWarnings("unchecked")
	private <T> List<T> readFromSeqFile(String destination) throws Exception {

		SequenceFile.Reader reader = new SequenceFile.Reader(sfFactoryBean.hdfsResourceLoader.getFileSystem(), new Path(
				destination), configuration);

		try {
			List<T> objects = new ArrayList<T>();

			while (reader.next((Object) null) != null) {

				Object currentValue = reader.getCurrentValue(reader.getValueClass().newInstance());

				if (currentValue instanceof AvroWrapper<?>) {
					currentValue = ((AvroWrapper<?>) currentValue).datum();
				}

				objects.add((T) currentValue);
			}

			return objects;

		} finally {
			IOUtils.closeStream(reader);
		}
	}

	private <T> List<T> readFromAvro(String destination) throws Exception {

		DataFileStream<T> reader = null;
		InputStream inputStream = null;
		try {
			HdfsResource hdfsResource = (HdfsResource) sfFactoryBean.hdfsResourceLoader.getResource(destination);

			inputStream = hdfsResource.getInputStream();

			reader = new DataFileStream<T>(inputStream, new ReflectDatumReader<T>());

			List<T> objects = new ArrayList<T>();

			for (T object : reader) {
				objects.add(object);
			}

			return objects;

		} finally {
			IOUtils.closeStream(reader);
			IOUtils.closeStream(inputStream);
		}
	}

	private void assertHdfsFileExists(String hdfsFile) {
		assertTrue("'" + hdfsFile + "' file is not present on HDFS.", sfFactoryBean.hdfsResourceLoader.getResource(hdfsFile)
				.exists());
	}

	public static class CustomCompressionCodec extends DefaultCodec {

		@Override
		public String getDefaultExtension() {
			return ".cusTom";
		}
	}

	public static <T> List<T> createPojoList(Class<T> objectClass, int size) throws Exception {

		List<T> objects = new ArrayList<T>();

		for (int i = 0; i < size; i++) {
			objects.add(objectClass.newInstance());
		}

		return objects;
	}

	public static class PojoSerializable implements Serializable {

		private static final long serialVersionUID = 4225081912489347353L;

		private static int COUNTER = 0;

		protected int id = COUNTER++;

		private String name = "[" + id + "]";

		private String description = "...here goes Pojo's description :)";

		private List<Integer> relations = new ArrayList<Integer>();
		{
			relations.add(id);
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getDescription() {
			return description;
		}

		public void setDescription(String description) {
			this.description = description;
		}

		public List<Integer> getRelations() {
			return relations;
		}

		public void setRelations(List<Integer> relations) {
			this.relations = relations;
		}

		@Override
		public String toString() {
			return getClass().getSimpleName() + ":" + name;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + id;
			result = prime * result + ((name == null) ? 0 : name.hashCode());
			result = prime * result + ((relations == null) ? 0 : relations.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			PojoSerializable other = (PojoSerializable) obj;
			if (id != other.id)
				return false;
			if (name == null) {
				if (other.name != null)
					return false;
			} else if (!name.equals(other.name))
				return false;
			if (relations == null) {
				if (other.relations != null)
					return false;
			} else if (!relations.equals(other.relations))
				return false;
			return true;
		}
	}

	public static class PojoWritable extends PojoSerializable implements Writable {

		private static final long serialVersionUID = -1196188141912933846L;

		public void write(DataOutput out) throws IOException {
			out.writeInt(id);
			out.writeUTF(getName());
			out.writeUTF(getDescription());

			{
				out.writeUTF(getRelations().getClass().getName());
				out.writeInt(getRelations().size());

				for (Integer relation : getRelations()) {
					out.writeInt(relation);
				}
			}
		}

		public void readFields(DataInput in) throws IOException {
			id = in.readInt();
			setName(in.readUTF());
			setDescription(in.readUTF());

			try {
				String listClassName = in.readUTF();

				Class<?> listClass = ClassUtils.resolveClassName(listClassName, getClass().getClassLoader());

				@SuppressWarnings("unchecked")
				List<Integer> relations = (List<Integer>) listClass.newInstance();

				for (int i = in.readInt(); i > 0; i--) {
					relations.add(in.readInt());
				}

				setRelations(relations);

			} catch (Exception e) {
				throw new RuntimeException(e.getMessage(), e);
			}
		}
	}

}
