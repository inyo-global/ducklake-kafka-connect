# IntelliJ IDEA Configuration - Imports and Formatting Guide

This document explains how to configure IntelliJ IDEA to work with Spotless and keep code consistent.

## Project Automatic Configuration

Project configuration files are already included in the `.idea/` folder:
- `.idea/codeStyles/codeStyleConfig.xml` - main configuration
- `.idea/inspectionProfiles/Project_Default.xml` - inspection profile

## Spotless Integration

The project uses **Spotless** as the primary formatting and code quality tool, replacing Checkstyle with several advantages:

### Advantages of Spotless vs Checkstyle:
- ✅ **Auto-fix**: Automatically corrects most issues
- ✅ **Google Java Format**: Consistent, modern formatting
- ✅ **Custom rules**: Flexibility for project-specific rules
- ✅ **Less configuration**: Fewer complex XML files
- ✅ **Native Gradle integration**: Better integration with the build

### Spotless commands:
```bash
./gradlew spotlessCheck    # Verifies formatting
./gradlew spotlessApply    # Applies fixes automatically
./gradlew check            # Runs all checks (includes Spotless)
```

## Manual Configuration (For other projects)

### 1. Configure Imports (Avoid wildcard imports)

1. Open **File → Settings** (or **IntelliJ IDEA → Preferences** on macOS)
2. Navigate to **Editor → Code Style → Java**
3. Go to the **Imports** tab
4. Configure the following options:

```
Class count to use import with '*': 999
Names count to use static import with '*': 999
```

5. Clear the list "Packages to Use Import with '*'" (leave empty)

### 2. Install Google Java Format Plugin

1. Go to **File → Settings → Plugins**
2. Search for "google-java-format"
3. Install and restart IntelliJ
4. Enable it in **Settings → google-java-format Settings**
5. Check "Enable google-java-format"

### 3. Configure Automatic Formatting

1. Under **Editor → Code Style → Java**
2. Configure:
   - **Right margin**: 120
   - **Wrap long lines**: ✓
   - **Tab size**: 2
   - **Indent**: 2
   - **Continuation indent**: 4

### 4. Enable Import Inspections

1. Go to **Editor → Inspections**
2. Search for "On demand import" and set it to **Error**
3. Search for "Unused import" and set it to **Warning**

## Recommended Workflow

### Before committing:
1. **Ctrl+Alt+L** (Cmd+Alt+L on Mac): Reformat code
2. **Ctrl+Alt+O** (Cmd+Alt+O on Mac): Optimize imports
3. Run: `./gradlew spotlessApply`
4. Run: `./gradlew check`

### During development:
- Configure "Actions on Save" to run formatting automatically
- Use the google-java-format plugin for consistent formatting

## Rules enforced by Spotless

The project enforces the following custom checks:

### 1. No wildcard imports
```
import java.util.*;  // ❌ Error
import java.util.List;  // ✅ Correct
```

### 2. Line length check
- Maximum 120 characters per line
- Exceptions: comments and URLs

### 3. Magic numbers check
- Numeric literals should be constants
```java
int size = 42;  // ❌ Error
private static final int DEFAULT_SIZE = 42;  // ✅ Correct
```

### 4. Automatic formatting
- Google Java Format
- Remove unused imports
- Trim trailing whitespace
- Ensure newline at end of files

## Troubleshooting

### If Spotless fails:
1. Run `./gradlew spotlessApply` to automatically fix issues
2. Check for wildcard imports
3. Check for excessively long lines
4. Check for magic numbers

### If IntelliJ doesn't format correctly:
1. Ensure the google-java-format plugin is enabled
2. Run **File → Invalidate Caches and Restart**
3. Ensure **Use per-project settings** is enabled

## CI/CD Integration

The pipeline automatically:
- ✅ Runs `spotlessCheck` to verify formatting
- ✅ Runs `spotbugsMain/Test` for static analysis
- ✅ Fails the build if formatting violations are present
- ✅ Produces code quality reports
