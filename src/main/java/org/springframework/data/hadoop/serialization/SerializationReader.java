/*
 * Copyright 2004-2012 the original author or authors.
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

import java.io.Closeable;
import java.io.IOException;

/**
 * A strategy interface encapsulating the logic to deserialize objects from HDFS.
 * 
 * <p>
 * Instances of this interface are created by {@link SerializationFormat}.
 * 
 * @see {@link SerializationWriter}
 * 
 * @author Alex Savov
 */
public interface SerializationReader<T> extends Closeable {

	/**
	 * Optional interface that might be implemented by <code>SerializationReader</code> that support sync marks.
	 */
	public static interface MarkSupport {

		/**
		 * Returns the position of last past sync mark.
		 */
		long lastMark() throws IOException;

		/**
		 * Positions the reader to a specified sync mark position.
		 */
		void gotoMark(long markPosition) throws IOException;

		/**
		 * The {@link SerializationReader#read() read} of a record might change the last sync mark. This method provides
		 * information whether the sync mark was located before or after that record. This is usefull if the client
		 * wants to keep track of items after last sync mark.
		 * 
		 * @return <code>true</code> if the sync mark is located before the record; <code>false</code> if the sync mark
		 * is located after the record.
		 */
		boolean isMarkAtRecordStart() throws IOException;

	}

	T read() throws IOException;

}
