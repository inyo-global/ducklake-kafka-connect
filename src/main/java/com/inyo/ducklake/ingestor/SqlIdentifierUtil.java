package com.inyo.ducklake.ingestor;

import java.util.regex.Pattern;

/**
 * Utility for validation and quoting of SQL identifiers (table and column names). Centralizes the
 * logic to avoid duplication and ensure consistent validation.
 */
public final class SqlIdentifierUtil {

  private static final Pattern IDENT_PATTERN = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

  private SqlIdentifierUtil() {}

  /**
   * Validates if the identifier follows the allowed pattern (without quotes) and returns the same
   * value.
   *
   * @param raw raw identifier
   * @return the identifier itself if valid
   * @throws IllegalArgumentException if invalid
   */
  public static String safeIdentifier(String raw) {
    if (raw == null || !IDENT_PATTERN.matcher(raw).matches()) {
      throw new IllegalArgumentException("Invalid identifier: " + raw);
    }
    return raw;
  }

  /**
   * Quotes identifiers that contain characters outside the simple pattern.
   *
   * @param identifier identifier
   * @return identifier possibly within double quotes, with internal escaping
   */
  public static String quote(String identifier) {
    if (identifier == null) {
      throw new IllegalArgumentException("Identifier cannot be null");
    }
    return identifier.matches("[a-zA-Z_][a-zA-Z0-9_]*")
        ? identifier
        : '"' + identifier.replace("\"", "\"\"") + '"';
  }
}
