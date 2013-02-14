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

package org.springframework.data.hadoop.batch;

import static org.springframework.util.StringUtils.hasText;

import java.io.IOException;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.Resource;
import org.springframework.data.hadoop.serialization.SerializationFormat;
import org.springframework.data.hadoop.serialization.SerializationReader;
import org.springframework.data.hadoop.serialization.SerializationReader.MarkSupport;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Spring Batch {@link ItemReader} implementation for reading data from a single HDFS resource using serialization
 * format.
 * 
 * @see {@link SerializationFormat}
 * 
 * @author Alex Savov
 */
public class HdfsSerializationFormatItemReader<T> extends AbstractItemCountingItemStreamItemReader<T> implements
		ResourceAwareItemReaderItemStream<T>, InitializingBean {

	/* The Reader provides core 'read objects from Hadoop' logic. Its lifecycle is demarcated by 'open-close' methods. */
	protected SerializationReader<T> serializationReader;

	/* A MarkSupport shortcut to/view of 'serializationReader'. */
	protected SerializationReader.MarkSupport markSupport;

	/* Position of last known sync mark. */
	protected long lastMark;

	/* Number of items read after last known sync mark. */
	protected int itemsAfterLastMark;

	protected final String lastMarkKey;
	protected final String itemsAfterLastMarkKey;

	//
	// The properties are publicly configurable.
	//

	/* The HDFS serialization format used to read objects. */
	private SerializationFormat<T> serializationFormat;

	/* HDFS location to read from. */
	private String location;

	/* HDFS resource to read from. */
	private Resource resource;

	{
		// Initialize the name for the key in the execution context.
		setName(ClassUtils.getShortName(getClass()));

		// Pre-calculate context keys.
		lastMarkKey = getExecutionContextUserSupport().getKey("lastMark");
		itemsAfterLastMarkKey = getExecutionContextUserSupport().getKey("itemsAfterLastMark");
	}

	//
	// Adapt Serialization Reader to Spring Batch Item Reader contract {{
	//

	/*
	 * Temporarily store 'open' context during 'open' method so 'jumpToItem' might access it.
	 * 
	 * Note: Not so good, but enforced by parent class design.
	 */
	protected ExecutionContext openExecutionContext;

	/*
	 * Call hierarchy: this.open -> super.open -> this.doOpen & this.jumpToItem -> super.jumpToItem
	 */
	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		try {
			// See 'openExecutionContext' comments.
			openExecutionContext = executionContext;

			super.open(executionContext);
		} finally {
			openExecutionContext = null;
		}
	}

	@Override
	protected void doOpen() throws IOException {
		if (hasText(location)) {

			serializationReader = serializationFormat.getReader(location);

		} else if (resource != null) {

			// Passed resource is used only to get its URI.
			serializationReader = serializationFormat.getReader(resource.getURI().toString());

		} else {
			Assert.state(false, "Set either 'location' or 'resource' property.");
		}

		if (serializationReader instanceof SerializationReader.MarkSupport) {
			// Create MarkSupport shortcut to the SerializationReader
			markSupport = (MarkSupport) serializationReader;
		} else {
			// Use dummy MarkSupport to omit the 'if's
			markSupport = DUMMY_MARK_SUPPORT;
		}

		// Init 'mark' related information
		lastMark = markSupport.lastMark();
		itemsAfterLastMark = 0;
	}

	@Override
	protected void jumpToItem(int itemIndex) throws Exception {

		// After restart.

		if (openExecutionContext.containsKey(lastMarkKey)) {

			lastMark = openExecutionContext.getLong(lastMarkKey);

			// Jump to last known sync marker
			markSupport.gotoMark(lastMark);

			// Read only items after last known sync marker (instead of all items starting from the beginning)
			itemIndex = openExecutionContext.getInt(itemsAfterLastMarkKey);

			// 'itemsAfterLastMark' will be updated by the reads called by jumpToItem
		}

		super.jumpToItem(itemIndex);
	}

	@Override
	protected T doRead() throws IOException {

		// Delegate to SerializationReader
		T read = serializationReader.read();

		long newLastMark = markSupport.lastMark();

		if (newLastMark == lastMark) {
			// the last sync mark is not changed by last read, so increase items counter
			itemsAfterLastMark++;
		} else {
			// a new last sync mark is available after last read, so update 'mark' related info
			lastMark = newLastMark;
			// the sync mark is at the start of the record -> so we've just read ONE item after the sync mark
			// the sync mark is at the end of the record -> so we've read NONE items after the sync mark
			itemsAfterLastMark = markSupport.isMarkAtRecordStart() ? 1 : 0;
		}

		return read;
	}

	@Override
	public void update(ExecutionContext executionContext) throws ItemStreamException {

		super.update(executionContext);

		if (isSaveState()) {
			executionContext.putLong(lastMarkKey, lastMark);
			executionContext.putInt(itemsAfterLastMarkKey, itemsAfterLastMark);
		}
	}

	@Override
	protected void doClose() throws IOException {
		serializationReader.close();
		serializationReader = null;
		markSupport = null;
		lastMark = -1;
		itemsAfterLastMark = -1;
	}

	// }}

	/**
	 * @param location The HDFS destination file path to read from.
	 */
	public void setLocation(String location) {
		this.location = location;
	}

	/**
	 * @param resource The {@link Resource} instance to read from.
	 */
	@Override
	public void setResource(Resource resource) {
		this.resource = resource;
	}

	/**
	 * @param serializationFormat The {@link SerializationFormat} instance used to read objects from Hadoop.
	 */
	public void setSerializationFormat(SerializationFormat<T> serializationFormat) {
		this.serializationFormat = serializationFormat;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(serializationFormat, "A non-null SerializationFormat is required.");
	}

	/* Dummy MarkSupport to omit the 'if's. */
	protected static final MarkSupport DUMMY_MARK_SUPPORT = new MarkSupport() {
		@Override
		public final long lastMark() throws IOException {
			return -1;
		}

		@Override
		public final void gotoMark(long markPosition) throws IOException {
			Assert.isTrue(lastMark() == markPosition,
					"Dummy MarkSupport must be called with the mark position returned by 'lastMark()'.");
		}

		@Override
		public boolean isMarkAtRecordStart() throws IOException {
			return false;
		}
	};

}
