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

import static org.apache.commons.io.FilenameUtils.EXTENSION_SEPARATOR;
import static org.apache.commons.io.FilenameUtils.getExtension;
import static org.apache.commons.io.FilenameUtils.removeExtension;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.fs.HdfsWrite.SerializationFormatSupport;
import org.springframework.data.hadoop.fs.HdfsWrite.SerializationKeyProvider;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Integration test for {@link HdfsWrite} testing simple and compressed writes of a file to HDFS.
 * 
 * @author Alex Savov
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class HdfsWriteTest {

	private static long timestamp;

	/*
	 * The instance under testing.
	 * 
	 * A NEW clean instance (scope = prototype) is injected by Spring runner for each test execution. See
	 * HdfsWriteTest-context.xml file.
	 */
	@Autowired
	private HdfsWrite hdfs;

	/**
	 * The file that's written to HDFS with/out compression.
	 */
	@Value("classpath:/data/apache-short.txt")
	private Resource source;

	/**
	 * All output files are written to that HDFS dir.
	 */
	@Value("${hdfs.write.output.dir}")
	private String hdfsOutputDir;

	/**
	 * Used as HDFS read-accessor to assert existence of written file.
	 */
	@Autowired
	private HdfsResourceLoader hdfsLoader;

	@Autowired
	private FileSystem fs;

	@Autowired
	private Configuration config;

	@BeforeClass
	public static void beforeClass() {
		timestamp = System.currentTimeMillis();
	}

	private SerializationFormatSupport AVRO;

	private SerializationFormatSupport SEQUENCE_FILE_JAVA;

	private SerializationFormatSupport SEQUENCE_FILE_WRITABLE;

	private SerializationFormatSupport SEQUENCE_FILE_AVRO;

	@Before
	public void initSerializationFormats() {
		
		AVRO = hdfs.new AvroWriter();

		SEQUENCE_FILE_JAVA = hdfs.new SequenceFileWriter();

		SEQUENCE_FILE_WRITABLE = hdfs.new SequenceFileWriter();

		SEQUENCE_FILE_AVRO = hdfs.new AvroSequenceFileWriter();		
	}
	
	/**
	 * Test write from source file to HDFS destination.
	 */
	@Test
	public void testWriteSimple() {

		final String destination = destination(source);

		hdfs.write(source, destination);

		assertHdfsFileExists(destination);
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

		hdfs.setSerializationKeyProvider(new SerializationKeyProvider<Void>() {

			public Void getKey(Object object) {
				return (Void) null;
			}

			public Class<Void> getKeyClass() {
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
	public void testCompressedWriteUsingHadoopCodecAlias() throws IOException {

		// DefaultCodec is configured by Hadoop by default
		final CompressionCodec codec = new CompressionCodecFactory(config).getCodecByName(DefaultCodec.class
				.getSimpleName());

		testCompressedWrite(codec, /* useCodecAlias */true);
	}

	/**
	 * Test compressed write from source file to HDFS destination using codec class name as configured within Hadoop.
	 */
	@Test
	public void testCompressedWriteUsingHadoopCodecClassName() throws IOException {

		// GzipCodec is configured by Hadoop by default
		final CompressionCodec codec = new CompressionCodecFactory(config).getCodecByName(GzipCodec.class
				.getSimpleName());

		testCompressedWrite(codec, /* useCodecAlias */false);
	}

	/**
	 * Test compressed write from source file to HDFS destination using user provided codec loaded from the classpath.
	 */
	@Test
	public void testCompressedWriteUsingUserCodecClassName() throws IOException {

		// CustomCompressionCodec is NOT supported by Hadoop, but is provided by the
		// client on the classpath
		final CompressionCodec codec = new CustomCompressionCodec();

		testCompressedWrite(codec, /* useCodecAlias */false);
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
		for (Class<? extends CompressionCodec> codecClass : CompressionCodecFactory.getCodecClasses(config)) {
			try {
				testCompressedWrite(ReflectionUtils.newInstance(codecClass, config), /* useCodecAlias */true);
			} catch (Exception exc) {
				exceptions.append(codecClass.getName() + " not supported. Details: " + exc.getMessage() + "\n");
			}
		}

		assertTrue(exceptions.toString(), exceptions.length() == 0);
	}

	/**
	 * Tests core compressed write logic. Although a codec is being passed as a parameter the method under testing is
	 * {@link HdfsWrite#write(Resource, String, String)}.
	 * 
	 * @param codec Used ONLY to get codec extension and its class name or alias in a type-safe manner.
	 * @param useAlias If <code>true</code> uses <code>codec.getClass().getSimpleName()</code> as a codec alias.
	 * Otherwise uses <code>codec.getClass().getName()</code> as a codec class name.
	 */
	private void testCompressedWrite(CompressionCodec codec, boolean useAlias) throws IOException {

		// calculates the destination from the source.
		final String destination = destination(source);

		// configure compression
		hdfs.setCodecAlias(useAlias ? codec.getClass().getSimpleName() : codec.getClass().getName());

		hdfs.write(source, destination);

		// expected destination on hdfs should have codec extension appended
		assertHdfsFileExists(destination + codec.getDefaultExtension());
	}

	/**
	 * @return Hdfs file destination calculated from the source. The file name is appended with the timestamp. The
	 * extension is kept the same.
	 */
	private String destination(Resource source) {

		String destination = hdfsOutputDir;

		// add file name
		destination += removeExtension(source.getFilename());
		// add time stamp
		destination += "_" + timestamp;
		// add file extension
		destination += EXTENSION_SEPARATOR + getExtension(source.getFilename());

		return destination;
	}

	/**
	 * @return Hdfs file destination calculated from the source. The file name is appended with the timestamp. The
	 * extension is kept the same.
	 */
	private String destination(Class<?> objectClass, SerializationFormatSupport serialization, boolean compress) {

		String destination = hdfsOutputDir;

		// add file name
		destination += objectClass.getSimpleName();
		// add time stamp
		destination += "_" + timestamp;
		// add compression
		destination += (compress ? "_compressed" : "");
		// add file extension specific to used serialization
		destination += serialization.getExtension();

		return destination;
	}

	private void assertHdfsFileExists(String hdfsFile) {
		assertTrue("'" + hdfsFile + "' file is not present on HDFS.", hdfsLoader.getResource(hdfsFile).exists());
	}

	private <T> void testSerializationWrite(Class<T> objectClass, SerializationFormatSupport serialization,
			boolean compress) throws Exception {

		List<T> objects = new ArrayList<T>();

		for (int i = 0; i < 5000; i++) {
			objects.add(objectClass.newInstance());
		}

		String destination = destination(objectClass, serialization, compress);

		hdfs.setSerializationFormat(serialization);

		if (compress) {
			// Use default Hadoop compression (via its alias) also supported by Avro!
			hdfs.setCodecAlias("deflate");
		}

		hdfs.write(objects, objectClass, destination);

		assertHdfsFileExists(destination);

		List<?> readObjects = serialization == AVRO ? readFromAvro(destination) : readFromSeqFile(destination);

		assertEquals(objects, readObjects);
	}

	@SuppressWarnings("unchecked")
	private <T> List<T> readFromSeqFile(String destination) throws Exception {

		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(destination), config);

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
			HdfsResource hdfsResource = (HdfsResource) hdfsLoader.getResource(destination);

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

	public static class CustomCompressionCodec extends DefaultCodec {

		@Override
		public String getDefaultExtension() {
			return ".cusTom";
		}
	}

	public static class PojoSerializable implements Serializable {

		private static final long serialVersionUID = 4225081912489347353L;

		private static int id = 0;

		private String name = "[" + (id++) + "] here goes Pojo's name";

		private String description = "...and here goes Pojo description :)";

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

		@Override
		public String toString() {
			return getClass().getSimpleName() + " [name=" + name + "]";
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((description == null) ? 0 : description.hashCode());
			result = prime * result + ((name == null) ? 0 : name.hashCode());
			return result;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			PojoSerializable other = (PojoSerializable) obj;
			if (description == null) {
				if (other.description != null)
					return false;
			} else if (!description.equals(other.description))
				return false;
			if (name == null) {
				if (other.name != null)
					return false;
			} else if (!name.equals(other.name))
				return false;
			return true;
		}
	}

	public static class PojoWritable extends PojoSerializable implements Writable {

		private static final long serialVersionUID = -1196188141912933846L;

		public void write(DataOutput out) throws IOException {
			out.writeUTF(getName());
			out.writeUTF(getDescription());
		}

		public void readFields(DataInput in) throws IOException {
			setName(in.readUTF());
			setDescription(in.readUTF());
		}
	}

}
