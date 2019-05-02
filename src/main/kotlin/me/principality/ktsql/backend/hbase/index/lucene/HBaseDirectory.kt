package me.principality.ktsql.backend.hbase.index.lucene

import org.apache.lucene.store.*

/**
 * 实现lucene的定制化存储
 */
class HBaseDirectory(lockFactory: LockFactory?) : BaseDirectory(lockFactory) {
    /**
     * Opens a stream for reading an existing file.
     *
     * This method must throw either [NoSuchFileException] or [FileNotFoundException]
     * if `name` points to a non-existing file.
     *
     * @param name the name of an existing file.
     * @throws IOException in case of I/O error
     */
    override fun openInput(name: String?, context: IOContext?): IndexInput {
        TODO("not implemented")
    }

    /**
     * Removes an existing file in the directory.
     *
     * This method must throw either [NoSuchFileException] or [FileNotFoundException]
     * if `name` points to a non-existing file.
     *
     * @param name the name of an existing file.
     * @throws IOException in case of I/O error
     */
    override fun deleteFile(name: String?) {
        TODO("not implemented")
    }

    /**
     * Creates a new, empty, temporary file in the directory and returns an [IndexOutput]
     * instance for appending data to this file.
     *
     * The temporary file name (accessible via [IndexOutput.getName]) will start with
     * `prefix`, end with `suffix` and have a reserved file extension `.tmp`.
     */
    override fun createTempOutput(prefix: String?, suffix: String?, context: IOContext?): IndexOutput {
        TODO("not implemented")
    }

    /**
     * Ensures that any writes to these files are moved to
     * stable storage (made durable).
     *
     * Lucene uses this to properly commit changes to the index, to prevent a machine/OS crash
     * from corrupting the index.
     *
     * @see .syncMetaData
     */
    override fun sync(names: MutableCollection<String>?) {
        TODO("not implemented")
    }

    /**
     * Returns names of all files stored in this directory.
     * The output must be in sorted (UTF-16, java's [String.compareTo]) order.
     *
     * @throws IOException in case of I/O error
     */
    override fun listAll(): Array<String> {
        TODO("not implemented")
    }

    /**
     * Ensures that directory metadata, such as recent file renames, are moved to stable
     * storage.
     *
     * @see .sync
     */
    override fun syncMetaData() {
        TODO("not implemented")
    }

    /**
     * Renames `source` file to `dest` file where
     * `dest` must not already exist in the directory.
     *
     * It is permitted for this operation to not be truly atomic, for example
     * both `source` and `dest` can be visible temporarily in [.listAll].
     * However, the implementation of this method must ensure the content of
     * `dest` appears as the entire `source` atomically. So once
     * `dest` is visible for readers, the entire content of previous `source`
     * is visible.
     *
     * This method is used by IndexWriter to publish commits.
     */
    override fun rename(source: String?, dest: String?) {
        TODO("not implemented")
    }

    /**
     * Creates a new, empty file in the directory and returns an [IndexOutput]
     * instance for appending data to this file.
     *
     * This method must throw [java.nio.file.FileAlreadyExistsException] if the file
     * already exists.
     *
     * @param name the name of the file to create.
     * @throws IOException in case of I/O error
     */
    override fun createOutput(name: String?, context: IOContext?): IndexOutput {
        TODO("not implemented")
    }

    /**
     * Returns the byte length of a file in the directory.
     *
     * This method must throw either [NoSuchFileException] or [FileNotFoundException]
     * if `name` points to a non-existing file.
     *
     * @param name the name of an existing file.
     * @throws IOException in case of I/O error
     */
    override fun fileLength(name: String?): Long {
        TODO("not implemented")
    }

    /**
     * Closes the directory.
     */
    override fun close() {
        TODO("not implemented")
    }
}