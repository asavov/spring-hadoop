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
package org.springframework.data.hadoop.serialization;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.file.CodecFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.ReflectionUtils;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * Compression codecs utility class.
 * 
 * @author Alex Savov
 */
public abstract class CompressionUtils {

	/**
	 * Resolve compression alias (such as 'snappy' or 'gzip') to Hadoop {@link CompressionCodec codec}.
	 * 
	 * @param conf Hadoop configuration to use.
	 * @param compressionAlias <ul>
	 * <li>The short class name (without the package) of the compression codec that is specified within Hadoop
	 * configuration (under <i>io.compression.codecs</i> prop). If the short class name ends with 'Codec', then there
	 * are two aliases for the codec - the complete short class name and the short class name without the 'Codec'
	 * ending. For example for the 'GzipCodec' codec class name the alias are 'gzip' and 'gzipcodec' (case insensitive).
	 * If the codec is configured to be used by Hadoop this is the preferred way instead of passing the codec canonical
	 * name.</li>
	 * <li>The canonical class name of the compression codec that is specified within Hadoop configuration (under
	 * <i>io.compression.codecs</i> prop) or is present on the classpath.</li>
	 * </ul>
	 * 
	 * @return The codec to be used to compress the data on the fly while storing it onto HDFS, if the
	 * <code>compressionAlias</code> property is specified; <code>null</code> otherwise.
	 * 
	 * @throws IllegalArgumentException if the codec class name could not be resolved
	 * @throws RuntimeException if the codec class could not be instantiated
	 */
	public static CompressionCodec getHadoopCompression(Configuration conf, String compressionAlias) {

		if (!StringUtils.hasText(compressionAlias)) {
			return null;
		}

		final CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);

		// Find codec by canonical class name or by codec alias as specified in Hadoop configuration
		CompressionCodec compression = codecFactory.getCodecByName(compressionAlias);

		// If the codec is not configured within Hadoop try to load it from the classpath
		if (compression == null) {
			Class<?> compressionClass = ClassUtils.resolveClassName(compressionAlias,
					SerializationWriter.class.getClassLoader());

			// Instantiate codec and initialize it from configuration
			// org.apache.hadoop.util.ReflectionUtils design is specific to Hadoop env :)
			compression = (CompressionCodec) ReflectionUtils.newInstance(compressionClass, conf);
		}

		return compression;
	}

	/**
	 * Resolve compression alias (such as 'snappy' or 'null') to Avro {@link CodecFactory codec}.
	 * 
	 * @param compressionAlias The compression alias.
	 * 
	 * @return <ul>
	 * <li>Avro codec as returned by {@link CodecFactory#fromString(String)} method, if the alias is not empty.</li>
	 * <li>'null' codec as returned by {@link CodecFactory#nullCodec()} method, if the alias is empty.</li>
	 * </ul>
	 * 
	 * @throws AvroRuntimeException if such a codec does not exist
	 */
	public static CodecFactory getAvroCompression(String compressionAlias) {
		return StringUtils.hasText(compressionAlias) ? CodecFactory.fromString(compressionAlias) : CodecFactory
				.nullCodec();
	}

}
