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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.hadoop.scripting.HdfsScriptRunner;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author Alex Savov
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class HdfsItemWriterTest {

	// @Costin: If the script runner is not autowired it's not executed at all, even configured to run at startup!
	// Any idea what's going on?
	@Autowired @Qualifier("cleanScript")
	private HdfsScriptRunner cleanScript;	

	@Autowired
	private JobLauncher jobLauncher;
	
	@Autowired @Qualifier("writeToHdfsJob-commit-interval")
	private Job writeToHdfsJob_commit_interval;
	
	@Autowired @Qualifier("writeToHdfsJob-default-completion")
	private Job writeToHdfsJob_default_completion;
	
	@Test
	public void testWriteToHdfsJob_commit_interval() throws Exception {
		jobLauncher.run(writeToHdfsJob_commit_interval, new JobParameters());
	}
	
	@Test
	public void testWriteToHdfsJob_default_completion() throws Exception {
		jobLauncher.run(writeToHdfsJob_default_completion, new JobParameters());
	}
}
