package ezdb.leveldb;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

import com.google.common.collect.ImmutableList;

public final class FileUtils {
	private static final int TEMP_DIR_ATTEMPTS = 10000;

	private FileUtils() {
	}

	public static boolean isSymbolicLink(final File file) {
		try {
			final File canonicalFile = file.getCanonicalFile();
			final File absoluteFile = file.getAbsoluteFile();
			final File parentFile = file.getParentFile();
			// a symbolic link has a different name between the canonical and absolute path
			return !canonicalFile.getName().equals(absoluteFile.getName()) ||
			// or the canonical parent path is not the same as the file's parent path,
			// provided the file has a parent path
					parentFile != null && !parentFile.getCanonicalPath().equals(canonicalFile.getParent());
		} catch (final IOException e) {
			// error on the side of caution
			return true;
		}
	}

	public static ImmutableList<File> listFiles(final File dir) {
		final File[] files = dir.listFiles();
		if (files == null) {
			return ImmutableList.of();
		}
		return ImmutableList.copyOf(files);
	}

	public static ImmutableList<File> listFiles(final File dir, final FilenameFilter filter) {
		final File[] files = dir.listFiles(filter);
		if (files == null) {
			return ImmutableList.of();
		}
		return ImmutableList.copyOf(files);
	}

	public static File createTempDir(final String prefix) {
		return createTempDir(new File(System.getProperty("java.io.tmpdir")), prefix);
	}

	public static File createTempDir(final File parentDir, final String prefix) {
		String baseName = "";
		if (prefix != null) {
			baseName += prefix + "-";
		}

		baseName += System.currentTimeMillis() + "-";
		for (int counter = 0; counter < TEMP_DIR_ATTEMPTS; counter++) {
			final File tempDir = new File(parentDir, baseName + counter);
			if (tempDir.mkdir()) {
				return tempDir;
			}
		}
		throw new IllegalStateException("Failed to create directory within " + TEMP_DIR_ATTEMPTS + " attempts (tried "
				+ baseName + "0 to " + baseName + (TEMP_DIR_ATTEMPTS - 1) + ')');
	}

	public static boolean deleteDirectoryContents(final File directory) {
		checkArgument(directory.isDirectory(), "Not a directory: %s", directory);

		// Don't delete symbolic link directories
		if (isSymbolicLink(directory)) {
			return false;
		}

		boolean success = true;
		for (final File file : listFiles(directory)) {
			success = deleteRecursively(file) && success;
		}
		return success;
	}

	public static boolean deleteRecursively(final File file) {
		boolean success = true;
		if (file.isDirectory()) {
			success = deleteDirectoryContents(file);
		}

		return file.delete() && success;
	}

	public static File newFile(final String parent, final String... paths) {
		requireNonNull(parent, "parent is null");
		requireNonNull(paths, "paths is null");

		return newFile(new File(parent), ImmutableList.copyOf(paths));
	}

	public static File newFile(final File parent, final String... paths) {
		requireNonNull(parent, "parent is null");
		requireNonNull(paths, "paths is null");

		return newFile(parent, ImmutableList.copyOf(paths));
	}

	public static File newFile(final File parent, final Iterable<String> paths) {
		requireNonNull(parent, "parent is null");
		requireNonNull(paths, "paths is null");

		File result = parent;
		for (final String path : paths) {
			result = new File(result, path);
		}
		return result;
	}
}
