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
package org.springframework.data.hadoop.batch;

import static org.junit.Assert.assertEquals;

import java.util.LinkedList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.support.IteratorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.hadoop.serialization.HdfsWriterTest;
import org.springframework.data.hadoop.serialization.HdfsWriterTest.PojoWritable;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Integration test for different HDFS specific Spring Batch Item Writers/Readers.
 * 
 * The test writes objects to HDFS, reads them back and asserts they are equal.
 * 
 * @author Alex Savov
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
// TODO: maybe split it.
public class HdfsSerializationFormatWriteReadTest {

	@Autowired
	private JobLauncher jobLauncher;

	@Autowired
	@Qualifier("hdfsItemWriterJob")
	private Job hdfsItemWriterJob;

	@Autowired
	@Qualifier("hdfsItemStreamWriterJob")
	private Job hdfsItemStreamWriterJob;

	@Autowired
	@Qualifier("hdfsMultiResourceItemWriterJob")
	private Job hdfsMultiResourceItemWriterJob;

	@Test
	public void hdfsItemWriterJob() throws Exception {

		writeReadTest(hdfsItemWriterJob);
	}

	@Test
	public void hdfsItemStreamWriterJob() throws Exception {

		writeReadTest(hdfsItemStreamWriterJob);
	}

	@Test
	public void hdfsMultiResourceItemWriterJob() throws Exception {

		writeReadTest(hdfsMultiResourceItemWriterJob);
	}

	protected void writeReadTest(Job writeReadJob) throws Exception {

		// Run "write-read" job.
		JobExecution job = jobLauncher.run(writeReadJob, new JobParameters());

		ExecutionContext jobContext = job.getExecutionContext();

		// Assert equality of original and read-back objects.
		assertEquals(jobContext.get(ObjectsReader.ORIGINAL_OBJECTS), jobContext.get(ObjectsWriter.OBJECTS_FROM_HDFS));
	}

	/**
	 * Provides objects that should be written to the HDFS.
	 */
	public static class ObjectsReader implements ItemReader<PojoWritable> {

		public final static String ORIGINAL_OBJECTS = "originalObjects";

		private ItemReader<PojoWritable> originalObjectsReader;

		private int objectsCount = 100;

		@BeforeStep
		public void beforeStep(StepExecution stepExecution) throws Exception {

			// The original objects to write.
			List<PojoWritable> originalObjects = HdfsWriterTest.createPojoList(PojoWritable.class, objectsCount);

			// Export/Share original objects for later usage.
			stepExecution.getJobExecution().getExecutionContext().put(ORIGINAL_OBJECTS, originalObjects);

			// Create core ItemReader to do the reading.
			originalObjectsReader = new IteratorItemReader<HdfsWriterTest.PojoWritable>(originalObjects);
		}

		/** Configure the number of objects to provide. */
		public void setObjectsCount(int objectsCount) {
			this.objectsCount = objectsCount;
		}

		@Override
		public PojoWritable read() throws Exception, UnexpectedInputException, ParseException,
				NonTransientResourceException {
			return originalObjectsReader.read();
		}
	}

	/**
	 * Store objects that are read back from HDFS.
	 */
	public static class ObjectsWriter implements ItemWriter<PojoWritable> {

		public static final String OBJECTS_FROM_HDFS = "objectsFromHdfs";

		private List<PojoWritable> objectsFromHdfs = new LinkedList<PojoWritable>();

		@AfterStep
		public void afterStep(StepExecution stepExecution) throws Exception {

			// Export/Share objects from HDFS for later usage.
			stepExecution.getJobExecution().getExecutionContext().put(OBJECTS_FROM_HDFS, objectsFromHdfs);
		}

		@Override
		public void write(List<? extends PojoWritable> items) throws Exception {
			objectsFromHdfs.addAll(items);
		}
	}

}
