package com.inyo.ducklake.ingestor;

/**
 * Constants for Arrow type bit widths and other common values used across the project. Centralizes
 * magic numbers to improve maintainability and consistency.
 */
public final class ArrowTypeConstants {

  private ArrowTypeConstants() {
    // Utility class - no instantiation
  }

  // Integer bit widths for Arrow types
  public static final int INT8_BIT_WIDTH = 8;
  public static final int INT16_BIT_WIDTH = 16;
  public static final int INT32_BIT_WIDTH = 32;
  public static final int INT64_BIT_WIDTH = 64;
}
