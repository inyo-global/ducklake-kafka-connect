/*
 * Copyright 2025 Inyo Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.inyo.ducklake.connect;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SpillDirectoryCleanupTest {

  @TempDir Path tempDir;

  private DucklakeSinkTask task;

  @BeforeEach
  void setUp() {
    task = new DucklakeSinkTask();
  }

  @Test
  void cleanupOrphanedSpillDirectories_removesOldStyleTempDirs() throws IOException {
    // Create orphaned directories matching old naming pattern (random suffix)
    Path orphaned1 = tempDir.resolve("ducklake-spill1234567890");
    Path orphaned2 = tempDir.resolve("ducklake-spillabc123def");
    Path orphaned3 = tempDir.resolve("ducklake-spill9999");
    Files.createDirectory(orphaned1);
    Files.createDirectory(orphaned2);
    Files.createDirectory(orphaned3);

    // Add some files inside to verify recursive deletion
    Files.writeString(orphaned1.resolve("spill-0.arrow"), "test data");
    Files.createDirectory(orphaned2.resolve("subdir"));
    Files.writeString(orphaned2.resolve("subdir").resolve("nested.arrow"), "nested data");

    // Verify directories exist before cleanup
    assertTrue(Files.exists(orphaned1));
    assertTrue(Files.exists(orphaned2));
    assertTrue(Files.exists(orphaned3));

    // Run cleanup
    task.cleanupOrphanedSpillDirectories(tempDir);

    // Verify orphaned directories were removed
    assertFalse(Files.exists(orphaned1), "orphaned1 should be deleted");
    assertFalse(Files.exists(orphaned2), "orphaned2 should be deleted");
    assertFalse(Files.exists(orphaned3), "orphaned3 should be deleted");
  }

  @Test
  void cleanupOrphanedSpillDirectories_preservesFixedSpillDirectory() throws IOException {
    // Create the new fixed spill directory (exact name match)
    Path fixedSpillDir = tempDir.resolve("ducklake-spill");
    Files.createDirectory(fixedSpillDir);
    Files.writeString(fixedSpillDir.resolve("active-data.arrow"), "active spill data");

    // Also create an orphaned directory
    Path orphaned = tempDir.resolve("ducklake-spill123");
    Files.createDirectory(orphaned);

    // Run cleanup
    task.cleanupOrphanedSpillDirectories(tempDir);

    // Fixed directory should be preserved
    assertTrue(Files.exists(fixedSpillDir), "fixed spill directory should be preserved");
    assertTrue(
        Files.exists(fixedSpillDir.resolve("active-data.arrow")),
        "files in fixed directory should be preserved");

    // Orphaned directory should be removed
    assertFalse(Files.exists(orphaned), "orphaned directory should be deleted");
  }

  @Test
  void cleanupOrphanedSpillDirectories_ignoresUnrelatedDirectories() throws IOException {
    // Create unrelated directories that should not be touched
    Path unrelated1 = tempDir.resolve("other-directory");
    Path unrelated2 = tempDir.resolve("kafka-connect-temp");
    Path unrelated3 = tempDir.resolve("ducklake-data"); // similar prefix but not spill
    Files.createDirectory(unrelated1);
    Files.createDirectory(unrelated2);
    Files.createDirectory(unrelated3);

    // Run cleanup
    task.cleanupOrphanedSpillDirectories(tempDir);

    // All unrelated directories should still exist
    assertTrue(Files.exists(unrelated1), "unrelated1 should not be touched");
    assertTrue(Files.exists(unrelated2), "unrelated2 should not be touched");
    assertTrue(Files.exists(unrelated3), "unrelated3 should not be touched");
  }

  @Test
  void cleanupOrphanedSpillDirectories_handlesEmptyTempDir() {
    // Should not throw on empty directory
    task.cleanupOrphanedSpillDirectories(tempDir);
    assertTrue(Files.exists(tempDir), "temp dir should still exist");
  }

  @Test
  void cleanupOrphanedSpillDirectories_handlesFilesNotDirectories() throws IOException {
    // Create a file (not directory) with matching name pattern
    Path spillFile = tempDir.resolve("ducklake-spill12345");
    Files.writeString(spillFile, "this is a file not a directory");

    // Run cleanup - should only target directories
    task.cleanupOrphanedSpillDirectories(tempDir);

    // File should still exist (cleanup only targets directories)
    assertTrue(Files.exists(spillFile), "files should not be deleted, only directories");
  }
}
