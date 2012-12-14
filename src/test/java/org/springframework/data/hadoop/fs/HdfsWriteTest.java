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
import static org.junit.Assert.assertTrue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Integration test for {@link HdfsWrite} testing simple and compressed writes of a
 * file to HDFS.
 *
 * @author Alex Savov
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class HdfsWriteTest {

    /* The instance under testing. */
    @Autowired
    private HdfsWrite hdfs;

    @Value("classpath:/data/apache-short.txt")
    private Resource source;

    @Autowired
    private HdfsResourceLoader hdfsLoader;

    @Autowired
    private Configuration config;

    /**
     * Test simple write from source to destionation.
     */
    @Test
    public void testWriteSimple() throws Exception {

        final String destination = destination();

        hdfs.write(source, destination);

        assertHdfsFileExists(destination);
    }

    /**
     * Test compressed write from source to destionation using codec alias as configured
     * within Hadoop.
     */
    @Test
    public void testWriteCompressedUsingHadoopCodecAlias() throws Exception {

        // DefaultCodec is configured by Hadoop by default
        final CompressionCodec codec = new CompressionCodecFactory(config).getCodecByName(DefaultCodec.class
                .getSimpleName());

        testWriteCompressed(codec, /* useCodecAlias */true);
    }

    /**
     * Test compressed write from source to destionation using codec class name as
     * configured within Hadoop.
     */
    @Test
    public void testWriteCompressedUsingHadoopCodecClassName() throws Exception {

        // GzipCodec is configured by Hadoop by default
        final CompressionCodec codec = new CompressionCodecFactory(config).getCodecByName(GzipCodec.class
                .getSimpleName());

        testWriteCompressed(codec, /* useCodecAlias */false);
    }

    /**
     * Test compressed write from source to destionation using user provided codec loaded
     * from the classpath.
     */
    @Test
    public void testWriteCompressedUsingUserCodecClassName() throws Exception {

        // BZip2Codec is NOT configured by Hadoop by default, although it's present on
        // the classpath
        final CompressionCodec codec = new BZip2Codec();

        // NOTE: I got "native library not loaded" when tried with Snappy/Lzop codec
        // cause I'm running on Windows, and it relies on Linux native library and
        // requires Hadoop native library support.
        // @Costin: If the build is running on Linux we might add those codecs too :)

        testWriteCompressed(codec, /* useCodecAlias */false);
    }

    /**
     * Core compressed write test logic.
     */
    private void testWriteCompressed(CompressionCodec codec, boolean useAlias) throws Exception {

        // calculates the destination from the source. adds timestamp to the destination
        // file name.
        final String destination = destination();

        final String codecAlias = useAlias ? codec.getClass().getSimpleName() : codec.getClass().getName();

        // do the write.
        hdfs.write(source, destination, codecAlias);

        // expected destination on hdfs should have codec extension appended
        assertHdfsFileExists(destination + codec.getDefaultExtension());
    }

    /**
     * @return Hdfs file destionation calculated from the source. The file name is
     *         appended with the timestamp. The extension is kept the same.
     */
    private String destination() {

        String destination = "/user/alex/files/_I_";

        // add file name
        destination += removeExtension(source.getFilename());
        // add time stamp
        destination += "_" + System.currentTimeMillis();
        // add file extension
        destination += EXTENSION_SEPARATOR + getExtension(source.getFilename());

        return destination;
    }

    private void assertHdfsFileExists(String hdfsFile) {
        assertTrue("'" + hdfsFile + "' file is not present on HDFS.", hdfsLoader.getResource(hdfsFile).exists());
    }

}
