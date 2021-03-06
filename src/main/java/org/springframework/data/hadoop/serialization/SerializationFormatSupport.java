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

import static org.apache.hadoop.io.IOUtils.closeStream;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

import org.springframework.data.hadoop.fs.HdfsResourceLoader;
import org.springframework.util.Assert;

/**
 * The class provides common functionality needed by {@link SerializationFormat} implementations. It includes support
 * for:
 * <ul>
 * <li>compression configuration;</li>
 * <li>extension customization;</li>
 * <li>HDFS resource loader configuration;</li>
 * <li>template classes for serialization format Writers and Readers.</li>
 * </ul>
 * 
 * @author Alex Savov
 */
public abstract class SerializationFormatSupport<T> implements SerializationFormat<T> {

	/* This property is publicly configurable. */
	private String compressionAlias;

	/* This property is publicly configurable. */
	private String extension;

	/* This property is publicly configurable. */
	// TODO [think about it]: Used ONLY by SF.Readers. It's not that good to be required in case of Writers.
	// - So is it OK to leave it optional or should it be required?
	// - So where/how to validate whether it's specified?
	// @ Costin: What you think?
	private HdfsResourceLoader hdfsResourceLoader;

	/* This property is publicly configurable. */
	protected boolean lazyOpenWriter = false;

	/* This property is publicly configurable. */
	protected boolean lazyOpenReader = false;

	/**
	 * Sets the compression alias for the <code>SerializationFormat</code>s created by this class. It's up to the
	 * implementation to resolve the alias to the actual compression algorithm.
	 * 
	 * @param compressionAlias The compression alias to use.
	 */
	public void setCompressionAlias(String compressionAlias) {
		this.compressionAlias = compressionAlias;
	}

	protected String getCompressionAlias() {
		return compressionAlias;
	}

	/**
	 * A flag indicating whether to open the writers upon {@link #getWriter(java.io.OutputStream) creation} or lazy-open
	 * them upon first {@link SerializationWriter#write(Object) write}.
	 * 
	 * <p>
	 * Default value is <code>false</code>.
	 * 
	 * @param lazyOpenWriter the lazyOpenWriter to set
	 */
	public void setLazyOpenWriter(boolean lazyOpenWriter) {
		this.lazyOpenWriter = lazyOpenWriter;
	}

	/**
	 * A flag indicating whether to open the readers upon {@link #getReader(String) creation} or lazy-open them upon
	 * first {@link SerializationReader#read() read}.
	 * 
	 * <p>
	 * Default value is <code>false</code>.
	 * 
	 * @param lazyOpenReader the lazyOpenReader to set
	 */
	public void setLazyOpenReader(boolean lazyOpenReader) {
		this.lazyOpenReader = lazyOpenReader;
	}

	/**
	 * Specify custom extension used/recognized by this serialization format. If <code>null</code> is set
	 * {@link #getDefaultExtension() default} extension is used.
	 * 
	 * @param extension the extension to set (such as '.myschema' or '.sf').
	 */
	public void setExtension(String extension) {
		this.extension = extension;
	}

	/**
	 * @return the extension set if it's not <code>null</code>; otherwise return {@link #getDefaultExtension() default}
	 * extension.
	 */
	@Override
	public String getExtension() {
		return extension != null ? extension : getDefaultExtension();
	}

	/**
	 * @return serailization format default non-null extension (such as '.avro', '.seqfile' or '.snappy')
	 */
	protected abstract String getDefaultExtension();

	/**
	 * Specify the loader to be used by this serialization format to access HDFS resources.
	 * 
	 * @param hdfsResourceLoader the hdfsResourceLoader to set
	 */
	public void setHdfsResourceLoader(HdfsResourceLoader hdfsResourceLoader) {
		this.hdfsResourceLoader = hdfsResourceLoader;
	}

	/**
	 * @return the hdfsResourceLoader
	 */
	protected HdfsResourceLoader getHdfsResourceLoader() {
		Assert.notNull(hdfsResourceLoader, "A non-null HdfsResourceLoader is required.");
		return hdfsResourceLoader;
	}

	@Override
	public SerializationWriter<T> getWriter(OutputStream output) throws IOException {

		SerializationWriterSupport writer = createWriter(output);

		if (!lazyOpenWriter) {
			writer.open();
		}

		return writer;
	}

	@Override
	public SerializationReader<T> getReader(String location) throws IOException {

		// TODO: Extract to utility class and do not couple to SerializationWriterObjectFactory!
		location = SerializationWriterObjectFactory.canonicalSerializationDestination(this, location);

		SerializationReaderSupport reader = createReader(location);

		if (!lazyOpenReader) {
			reader.open();
		}

		return reader;
	}

	/**
	 * Should be implemented by descendant classes. Used by core {@link #getWriter(OutputStream)} method.
	 */
	protected abstract SerializationWriterSupport createWriter(OutputStream output);

	/**
	 * Should be implemented by descendant classes. Used by core {@link #getReader(String)} method.
	 */
	protected abstract SerializationReaderSupport createReader(String location);

	/**
	 * A template class to be extended by <code>SerializationFormatWriter</code> implementations. Descendants should
	 * focus on {@link #doWrite(Object)} method.
	 */
	protected abstract class SerializationWriterSupport extends OpenCloseSupport implements SerializationWriter<T> {

		/**
		 * <ul>
		 * <li>Lazy open the Writer upon first write (if not already opened).</li>
		 * <li>Delegate to {@link #doWrite(Object) core} serialization logic.</li>
		 * </ul>
		 */
		@Override
		public void write(T source) throws IOException {

			// Lazy open serialization writer upon first write (if not already opened)
			open();

			// Delegate to core serialization logic.
			doWrite(source);
		}

		/**
		 * Here goes core serialization logic. The Writer is guaranteed to be open prior this call.
		 * @param source The object to write.
		 */
		protected abstract void doWrite(T source) throws IOException;
	}

	/**
	 * A template class to be extended by <code>SerializationReaderSupport</code> implementations. Descendants should
	 * focus on {@link #doRead()} method.
	 */
	protected abstract class SerializationReaderSupport extends OpenCloseSupport implements SerializationReader<T> {

		/**
		 * <ul>
		 * <li>Lazy open the Reader upon first read (if not already opened).</li>
		 * <li>Delegate to {@link #doRead() core} deserialization logic.</li>
		 * <li>Auto-close the Reader if read object is <code>null</code>.</li>
		 * </ul>
		 */
		@Override
		public T read() throws IOException {

			// Lazy open serialization reader upon first read (if not already opened)
			open();

			// Delegate to core serialization
			T object = doRead();

			if (object == null) {
				close();
			}

			return object;
		}

		/**
		 * Here goes core deserialization logic. The Reader is guaranteed to be open prior this call.
		 * @return The object that's read.
		 */
		protected abstract T doRead() throws IOException;
	}

	/**
	 * Open-Close utility class used by Readers and Writers.
	 */
	protected static abstract class OpenCloseSupport implements Closeable {

		/* Indicates whether this serialization format has been opened. */
		protected boolean isOpen = false;

		/* The native resource used by this serialization format that should be released upon close(). */
		protected Closeable nativeResource;

		/**
		 * Opens this serialization format. The method silently returns if the serialization format is already opened.
		 */
		protected void open() throws IOException {
			if (!isOpen) {
				nativeResource = doOpen();

				isOpen = true;
			}
		}

		/**
		 * Closes the native resource returned by {@link #doOpen()}.
		 */
		@Override
		public void close() throws IOException {
			closeStream(nativeResource);
			nativeResource = null;
			isOpen = false;
		}

		/**
		 * A hook method to be implemented by descendants to execute custom open logic.
		 * @return The underlying native resource (as Closeable) to be released by {@link #close()}.
		 */
		protected abstract Closeable doOpen() throws IOException;

	}

}