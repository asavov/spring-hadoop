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

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.io.IOUtils;

/**
 * Serialization format for writing POJOs using <code>Avro</code> serialization.
 * 
 * @author Alex Savov
 */
public class AvroFormat<T> extends AbstractObjectsSerializationFormat<T> {

	public AvroFormat(Class<T> objectsClass) {
		super(objectsClass);
	}

	@Override
	public void serialize(Iterable<? extends T> objects, OutputStream outputStream) throws IOException {
		DataFileWriter<T> writer = null;
		try {
			Schema schema = ReflectData.get().getSchema(objectsClass);

			writer = new DataFileWriter<T>(new ReflectDatumWriter<T>(schema));

			writer.setCodec(CompressionUtils.getAvroCompression(getCompressionAlias()));

			writer.create(schema, outputStream);

			for (T object : objects) {
				writer.append(object);
			}
		} finally {
			IOUtils.closeStream(writer);
		}
	}

	/**
	 * @return <b>.avro</b> is the default file extension for Avro serialization.
	 */
	@Override
	public String getExtension() {
		return ".avro";
	}

}
