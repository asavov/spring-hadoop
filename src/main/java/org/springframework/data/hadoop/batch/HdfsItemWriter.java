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

import java.util.List;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.item.file.ResourceSuffixCreator;
import org.springframework.data.hadoop.serialization.HdfsWriter;

/**
 * 
 * @author Alex Savov
 */
public class HdfsItemWriter<T> implements ItemStreamWriter<T> {

	private HdfsWriter hdfsWriter;

	private int calls = 0;
	private String destination;
	
	private ResourceSuffixCreator destinationSuffixCreator; 

	@Override
	public void write(List<? extends T> items) throws Exception {
		
		String destWithSuffix = destination;
		
		if (destinationSuffixCreator != null) {
			destWithSuffix += destinationSuffixCreator.getSuffix(calls++);
		}
		
		hdfsWriter.write(items, destWithSuffix);
	}

	/**
	 * @param hdfsWriter the hdfsWriter to set
	 */
	public void setHdfsWriter(HdfsWriter hdfsWriter) {
		this.hdfsWriter = hdfsWriter;
	}
	
	public void setDestination(String destination) {
		this.destination = destination;
	}
	
	public void setDestinationSuffixCreator(ResourceSuffixCreator destinationSuffixCreator) {
		this.destinationSuffixCreator = destinationSuffixCreator;
	}	
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.batch.item.ItemStream#open(org.springframework.batch.item.ExecutionContext)
	 */
	@Override
	public void open(ExecutionContext executionContext) throws ItemStreamException {
		System.out.println(">>> open");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.batch.item.ItemStream#update(org.springframework.batch.item.ExecutionContext)
	 */
	@Override
	public void update(ExecutionContext executionContext) throws ItemStreamException {
		System.out.println(">>> update");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.batch.item.ItemStream#close()
	 */
	@Override
	public void close() throws ItemStreamException {
		System.out.println(">>> close");
	}

}
