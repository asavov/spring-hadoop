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

import static org.apache.hadoop.io.IOUtils.closeStream;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
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
import org.springframework.data.hadoop.fs.HdfsResourceLoader;
import org.springframework.data.hadoop.serialization.SerializationFormatOperations.SerializationWriterCallback;
import org.springframework.expression.AccessException;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.ClassUtils;

/**
 * Integration test for {@link SerializationFormat} testing simple and compressed writes of a file and objects to HDFS.
 * 
 * @author Alex Savov
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class HdfsWriterTest {

	@Autowired
	private HdfsResourceLoader hdfsResourceLoader;

	@Autowired
	private Configuration configuration;

	/* The file that's written to HDFS with[out] compression. */
	@Value("classpath:/data/apache-short.txt")
	private Resource sourceResource;

	/* All output files are written to that HDFS dir. */
	@Value("${hdfs.writer.output.dir}")
	private String hdfsOutputDir;

	private SerializationWriterObjectFactory sfObjectFactory;

	private ResourceSerializationFormat RESOURCE_FORMAT;

	private AvroFormat<PojoSerializable> AVRO;

	private SequenceFileFormat<PojoSerializable> SEQUENCE_FILE_JAVA;

	private SequenceFileFormat<PojoWritable> SEQUENCE_FILE_WRITABLE;

	private AvroSequenceFileFormat<PojoSerializable> SEQUENCE_FILE_AVRO;

	@Before
	public void setUp() throws Exception {

		sfObjectFactory = new SerializationWriterObjectFactory(hdfsResourceLoader);

		RESOURCE_FORMAT = new ResourceSerializationFormat();
		RESOURCE_FORMAT.setConfiguration(configuration);
		RESOURCE_FORMAT.afterPropertiesSet();

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

		SEQUENCE_FILE_WRITABLE = new SequenceFileFormat<PojoWritable>(PojoWritable.class);
		SEQUENCE_FILE_WRITABLE.setConfiguration(configuration);
		SEQUENCE_FILE_WRITABLE.afterPropertiesSet();

		SEQUENCE_FILE_JAVA = new SequenceFileFormat<PojoSerializable>(PojoSerializable.class);
		SEQUENCE_FILE_JAVA.setConfiguration(configuration);
		SEQUENCE_FILE_JAVA.afterPropertiesSet();

		SEQUENCE_FILE_AVRO = new AvroSequenceFileFormat<PojoSerializable>(PojoSerializable.class);
		SEQUENCE_FILE_AVRO.setConfiguration(configuration);
		SEQUENCE_FILE_AVRO.afterPropertiesSet();

		AVRO = new AvroFormat<PojoSerializable>(PojoSerializable.class);
	}

	/*
	 * Test write from source file to HDFS destination.
	 */
	@Test
	public void testWriteOfResource() throws Exception {

		testResourceWrite(1, /* no compression */null, /* doesnt matter */false);
	}

	/*
	 * Test write from multiple source files (in our case the same one) to HDFS destination.
	 */
	@Test
	public void testWriteOfMultipleResources() throws Exception {
		testResourceWrite(5, /* no compression */null, /* doesnt matter */false);
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

		testResourceWrite(1, codec, /* useCodecAlias */true);
	}

	/**
	 * Test compressed write from source file to HDFS destination using codec class name as configured within Hadoop.
	 */
	@Test
	public void testCompressedWriteUsingHadoopCodecClassName() throws Exception {

		// GzipCodec is configured by Hadoop by default
		final CompressionCodec codec = new CompressionCodecFactory(configuration).getCodecByName(GzipCodec.class
				.getSimpleName());

		testResourceWrite(1, codec, /* useCodecAlias */false);
	}

	/**
	 * Test compressed write from source file to HDFS destination using user provided codec loaded from the classpath.
	 */
	@Test
	public void testCompressedWriteUsingUserCodecClassName() throws Exception {

		// CustomCompressionCodec is NOT supported by Hadoop, but is provided by the
		// client on the classpath
		final CompressionCodec codec = new CustomCompressionCodec();

		testResourceWrite(1, codec, /* useCodecAlias */false);
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
				testResourceWrite(1, ReflectionUtils.newInstance(codecClass, configuration), /* useCodecAlias */true);
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
	private void testResourceWrite(int resourceCopies, CompressionCodec codec, boolean useAlias) throws Exception {

		if (codec != null) {
			// configure compression
			RESOURCE_FORMAT.setCompressionAlias(useAlias ? codec.getClass().getSimpleName() : codec.getClass()
					.getName());
		}

		// calculates the destination for Resource source.
		String destination;
		{
			destination = hdfsOutputDir;

			// add file name
			destination += sourceResource.getFilename();
			// add files count
			destination += "_" + resourceCopies;
			// add serialization format name
			destination += "_" + RESOURCE_FORMAT.getClass().getSimpleName();
		}

		hdfsWrite(RESOURCE_FORMAT, Collections.nCopies(resourceCopies, sourceResource), destination);

		// expected destination on hdfs should have codec extension appended
		assertHdfsFileExists(destination + RESOURCE_FORMAT.getExtension());
	}

	/**
	 * Test core write-of-objects logic.
	 */
	private <T> void testSerializationWrite(Class<T> objectClass, SerializationFormatSupport<T> serializationCreator,
			boolean compress) throws Exception {

		List<T> objects = createPojoList(objectClass, 5000);

		String destination;
		{
			destination = hdfsOutputDir;

			// add class name
			destination += objectClass.getSimpleName();
			// add objects count
			destination += "_" + objects.size();
			// add serialization format name
			destination += "_" + serializationCreator.getClass().getSimpleName();
			// add compression flag
			destination += (compress ? "_compressed" : "");
			// add serialization format extension
			destination += serializationCreator.getExtension();
		}

		if (compress) {
			// Use default Hadoop compression (via its alias) also supported by Avro!
			serializationCreator.setCompressionAlias("deflate");
		}

		hdfsWrite(serializationCreator, objects, destination);

		assertHdfsFileExists(destination);

		List<T> readObjects = new ArrayList<T>();
		{
			// We do need that while reading (as opposite to writing)!
			serializationCreator.setHdfsResourceLoader(hdfsResourceLoader);

			SerializationReader<T> reader = serializationCreator.getReader(destination);

			for (T readObject = reader.read(); readObject != null; readObject = reader.read()) {
				readObjects.add(readObject);
			}

			closeStream(reader);
		}

		assertEquals(objects, readObjects);
	}

	private <T> void hdfsWrite(SerializationFormat<T> serializationCreator, final Iterable<T> sources,
			String destination) throws Exception {

		// Delegate to core SerializationFormat logic.
		sfObjectFactory.setSerializationFormat(serializationCreator);

		new SerializationFormatTemplate(sfObjectFactory).write(destination, new SerializationWriterCallback<T>() {
			@Override
			public void doInSerializationFormat(SerializationWriter<T> serializationFormat) throws IOException {
				for (T source : sources) {
					serializationFormat.write(source);
				}
			}
		});
	}

	private void assertHdfsFileExists(String hdfsFile) {
		assertTrue("'" + hdfsFile + "' file is not present on HDFS.", hdfsResourceLoader.getResource(hdfsFile).exists());
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
