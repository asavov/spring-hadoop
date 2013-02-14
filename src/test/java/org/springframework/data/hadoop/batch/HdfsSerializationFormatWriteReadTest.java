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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.support.IteratorItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.hadoop.serialization.HdfsWriterTest;
import org.springframework.data.hadoop.serialization.HdfsWriterTest.PojoWritable;
import org.springframework.data.hadoop.serialization.SerializationFormatSupport;
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
// TODO: Split it!
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

	@Autowired
	@Qualifier("restartableReaderTest")
	private Job restartableReaderTest;

	@Autowired
	@Qualifier("restartableReaderTest_ItemReader")
	private FailingHdfsSerializationFormatItemReaderTest<PojoWritable> restartableReader;

	@Autowired
	private SerializationFormatSupport<PojoWritable> serializationFormatRef;

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
		JobParameters jobParameters = new JobParameters();

		JobExecution jobExecution = jobLauncher.run(writeReadJob, jobParameters);

		ExecutionContext jobContext = jobExecution.getExecutionContext();

		// Assert equality of original and read-back objects.
		assertEquals(jobContext.get(ObjectsReader.ORIGINAL_OBJECTS), jobContext.get(ObjectsWriter.OBJECTS_FROM_HDFS));
	}

	@Test
	public void restartableReaderTest() throws Exception {

		// Execute the test with different compression strategies
		for (String compressionAlias : Arrays.asList("none", "deflate")) {

			// Configure singleton Serialization Format instance used by Writers and Readers
			serializationFormatRef.setCompressionAlias(compressionAlias);

			// Run the job that writes objects to HDFS
			JobExecution job = jobLauncher.run(restartableReaderTest,
					new JobParameters(Collections.singletonMap("compression", new JobParameter(compressionAlias))));

			ExecutionContext jobContext = job.getExecutionContext();

			// Get the original list of objects written
			@SuppressWarnings("unchecked")
			List<PojoWritable> originalObjects = (List<PojoWritable>) jobContext.get(ObjectsReader.ORIGINAL_OBJECTS);

			ExecutionContext readContext = new ExecutionContext();

			Iterator<PojoWritable> originalObjectsIterator = originalObjects.iterator();

			// Retry reads until all items are read or something goes wrong :)
			while (!assertReads(restartableReader, readContext, originalObjectsIterator)) {
				; // Do nothing
			}
		}
	}

	protected boolean assertReads(ItemStreamReader<PojoWritable> reader, ExecutionContext readContext,
			Iterator<?> originalObjects) throws Exception {

		System.out.println("--- [ItemReadTest] START");

		// Mimic Spring Batch contract

		reader.open(readContext);
		reader.update(readContext);

		try {
			while (true) {
				PojoWritable read;
				try {
					read = reader.read();
				} catch (UnexpectedInputException e) {
					System.out.println("--- [ItemReadTest] FAILED");
					return false;
				}

				if (read == null) {
					return true;
				}

				reader.update(readContext);

				// Compare the object read from HDFS with original one!
				assertEquals(originalObjects.next(), read);
			}
		} finally {
			reader.close();
		}
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

	/**
	 * Helper ItemReader testing the 'restartability' of core <code>HdfsSerializationFormatItemReader</code>.
	 * 
	 * Use extension (as opposite to composition) for the sake of easability.
	 */
	public static class FailingHdfsSerializationFormatItemReaderTest<T> extends
			FailingHdfsSerializationFormatItemReader<T> {

		@Override
		public void open(ExecutionContext executionContext) {

			super.open(executionContext);

			// on 'shouldFail' cycle we start clear and fail on second item
			// on 'non-shouldFail' cycle we restart after a failure
			boolean shouldFail = Boolean.valueOf(ecRef.getString("shouldFail"));

			int expectedItemsAfterLastMark = shouldFail ? 0 : SECOND_ITEM_AFTER_LAST_SYNC;

			assertNotNull("'lastMark' is not init after open.", lastMark);
			assertEquals("'itemsAfterLastMark' is not init after open.", expectedItemsAfterLastMark, itemsAfterLastMark);
		}

		@Override
		public void update(ExecutionContext ec) throws ItemStreamException {

			super.update(ec);

			assertTrue("'lastMark' is not stored in the context.", ec.containsKey(lastMarkKey));
			assertTrue("'itemsAfterLastMark' is not stored in the context.", ec.containsKey(itemsAfterLastMarkKey));
		}

		@Override
		protected T doRead() throws IOException {

			long old_lastMark = lastMark;
			int old_itemsAfterLastMark = itemsAfterLastMark;

			T read = super.doRead();

			long new_lastMark = lastMark;
			int new_itemsAfterLastMark = itemsAfterLastMark;

			if (old_lastMark == new_lastMark) {
				assertEquals(old_itemsAfterLastMark + 1, new_itemsAfterLastMark);
			} else {
				// we've passed a sync marker.
				// - the sync mark is at the start of the record -> so we've just read ONE item after the sync mark
				// - the sync mark is at the end of the record -> so we've read NONE items after the sync mark
				assertEquals(markSupport.isMarkAtRecordStart() ? 1 : 0, new_itemsAfterLastMark);
			}

			return read;
		}
	}

	/**
	 * A wrapper/extension around standard <code>HdfsSerializationFormatItemReader</code> which fails on second item
	 * after last sync mark. On restart the second (failed) item is successfully read and the reader keeps reading to
	 * the second item after next sync mark where it fails again...and so on until the end is reached.
	 * 
	 * Use extension (as opposite to composition) for the sake of easability.
	 */
	public static class FailingHdfsSerializationFormatItemReader<T> extends HdfsSerializationFormatItemReader<T> {

		static final int SECOND_ITEM_AFTER_LAST_SYNC = 1;

		// on 'shouldFail' cycle we start clear and fail on second item
		// on 'non-shouldFail' cycle we restart after a failure and successfully read second item

		static final String SHOULD_FAIL_KEY = "shouldFail";

		ExecutionContext ecRef;

		@Override
		public void open(ExecutionContext executionContext) {

			super.open(executionContext);

			ecRef = executionContext;

			if (!ecRef.containsKey(SHOULD_FAIL_KEY)) {
				ecRef.putString(SHOULD_FAIL_KEY, Boolean.TRUE.toString());
			}
		}

		@Override
		protected T doRead() throws IOException {

			if (Boolean.valueOf(ecRef.getString(SHOULD_FAIL_KEY)) && itemsAfterLastMark == SECOND_ITEM_AFTER_LAST_SYNC) {

				// Set that next cycle is 'non-shouldFail'
				ecRef.putString(SHOULD_FAIL_KEY, Boolean.FALSE.toString());

				throw new UnexpectedInputException(
						"failure on every second item (at position 1) after last sync marker.");
			}

			long old_lastMark = lastMark;

			T read = super.doRead();

			long new_lastMark = lastMark;

			if (old_lastMark != new_lastMark) {
				// we've passed a sync marker.
				// set that next cycle is 'shouldFail'
				ecRef.putString(SHOULD_FAIL_KEY, Boolean.TRUE.toString());
			}

			return read;
		}

	}
}
