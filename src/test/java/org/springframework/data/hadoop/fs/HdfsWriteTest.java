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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
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
	 * A NEW instance (scope = prototype) is injected by Spring runner for each test execution.
	 * See HdfsWriteTest-context.xml file.
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

	/**
	 * Test write from source file to HDFS destination.
	 */
	@Test
	public void testWriteSimple() throws Exception {

		final String destination = destination(source);

		hdfs.write(source, destination);

		assertHdfsFileExists(destination);
	}

	/**
	 * Test write of single pojo using Writable serialization.
	 */
	@Test
	public void testWriteToSeqFileWritable() throws Exception {

		testWriteToSeqFile(new PojoWritable(), /* compress */false);
	}

	/**
	 * Test compression write of single pojo using Writable serialization.
	 */
	@Test
	public void testCompressedWriteToSeqFileWritable() throws Exception {

		testWriteToSeqFile(new PojoWritable(), /* compress */true);
	}

	/**
	 * Test write of single pojo using Java serialization.
	 */
	@Test
	public void testWriteToSeqFileSerializable() throws Exception {

		testWriteToSeqFile(new PojoSerializable(), /* compress */false);
	}

	/**
	 * Test compressed write of single pojo using Java serialization.
	 */
	@Test
	public void testCompressedWriteToSeqFileSerializable() throws Exception {

		testWriteToSeqFile(new PojoSerializable(), /* compress */true);
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
		/*
		 * @Costin: Is that what you proposed - a test against all Hadoop codecs?
		 * 
		 * TODO: Needs to be re-worked to support parameterized tests. See @Parameterized and Parameterized.Parameters
		 */

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
	private String destination(Object javaObject, boolean compress) {

		String destination = hdfsOutputDir;

		// add file name
		destination += javaObject.getClass().getSimpleName();
		// add time stamp
		destination += "_" + timestamp;
		// add compression
		destination += (compress ? "_compressed" : "");
		// add SEQ file extension
		destination += HdfsWrite.SEQ_FILE_EXTENSION;

		return destination;
	}

	private void assertHdfsFileExists(String hdfsFile) {
		assertTrue("'" + hdfsFile + "' file is not present on HDFS.", hdfsLoader.getResource(hdfsFile).exists());
	}

	private <T> void testWriteToSeqFile(T javaObject, boolean compress) throws Exception {

		String destination = destination(javaObject, compress);

		if (compress) {
			// Use default hadoop compression
			hdfs.setCodecAlias(DefaultCodec.class.getSimpleName());
		}

		// @Costin, how to do the right 'casts' here? Or how to change the method under testing?
		hdfs.write(Collections.singletonList(javaObject), (Class<T>) javaObject.getClass(), destination);

		assertHdfsFileExists(destination);

		assertEquals(javaObject, readFromSeqFile(destination));
	}

	private Object readFromSeqFile(String destination) throws Exception {

		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(destination), config);

		try {
			Object dummyKey = reader.next((Object) NullWritable.get());

			assertSame(NullWritable.get(), dummyKey);

			return reader.getCurrentValue(reader.getValueClass().newInstance());

		} finally {
			reader.close();
		}
	}

	public static class CustomCompressionCodec extends DefaultCodec {

		@Override
		public String getDefaultExtension() {
			return ".cusTom";
		}

	}

	public static class Pojo {

		private String name = "here goes Pojo's name";

		private String description = "and here goes Pojo's description :)";

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
			Pojo other = (Pojo) obj;
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

	public static class PojoWritable extends Pojo implements Writable {

		public void write(DataOutput out) throws IOException {
			out.writeUTF(getName());
			out.writeUTF(getDescription());
		}

		public void readFields(DataInput in) throws IOException {
			setName(in.readUTF());
			setDescription(in.readUTF());
		}
	}

	public static class PojoSerializable extends Pojo implements Serializable {

		private static final long serialVersionUID = -1183104200586999767L;

	}

	public static class PojoExternalizble extends Pojo implements Externalizable {

		public void writeExternal(ObjectOutput out) throws IOException {
			out.writeUTF(getName());
			out.writeUTF(getDescription());
		}

		public void readExternal(ObjectInput in) throws IOException {
			setName(in.readUTF());
			setDescription(in.readUTF());
		}

	}
}
