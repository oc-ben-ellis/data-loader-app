#!/usr/bin/env python3
"""Custom pre-commit hook to enforce PascalCase for class names.

This script checks that class names follow proper PascalCase conventions,
specifically ensuring that acronyms are written as 'Sftp' instead of 'SFTP'.
"""

import re
import sys
from pathlib import Path


def check_class_naming(file_path: Path) -> list[tuple[int, str, str]]:
    """Check a Python file for class naming violations.

    Returns a list of tuples: (line_number, class_name, suggestion)
    """
    violations = []

    # Pattern to match class definitions
    class_pattern = re.compile(r"^class\s+([A-Z][A-Za-z0-9]*):")

    try:
        with open(file_path, encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                match = class_pattern.match(line.strip())
                if match:
                    class_name = match.group(1)

                    # Check if this looks like an acronym followed by lowercase
                    # Examples: SFTPLoader, HTTPManager
                    if re.search(r"[A-Z]{2,}[a-z]", class_name):
                        # Check if this contains any non-common acronyms that should be converted
                        if contains_non_common_acronyms(class_name):
                            # Generate suggestion by converting acronyms to PascalCase
                            suggestion = convert_to_pascal_case(class_name)
                            violations.append((line_num, class_name, suggestion))

    except Exception:
        pass

    return violations


def contains_non_common_acronyms(class_name: str) -> bool:
    """Check if a class name contains any non-common acronyms that should be converted.

    Examples:
        SFTPLoader -> False (SFTP is a common acronym)
        HTTPManager -> False (HTTP is a common acronym)
        OAuthProvider -> False (OAUTH is a common acronym)
        TestUSFloridaFunctional -> True (US is not a common acronym)
    """
    # Common acronyms that should remain as-is
    common_acronyms = {
        "SFTP",
        "FTP",
        "FTPS",
        "HTTP",
        "HTTPS",
        "HTTPX",
        "OAUTH",
        "SSH",
        "SSL",
        "TLS",
        "API",
        "URL",
        "URI",
        "JSON",
        "XML",
        "CSV",
        "YAML",
        "ZIP",
        "GZIP",
        "BZIP",
        "LZMA",
        "AWS",
        "CLI",
        "WSGI",
        "KV",
    }

    # Handle special cases first
    special_cases = ["OAuth"]
    for special_case in special_cases:
        if special_case in class_name:
            return False  # OAuth is a special case that should remain as-is

    # Use a simpler approach with regex
    # Look for patterns like: SFTP in SFTPLoader, HTTP in HTTPManager
    # We want to find acronyms that are followed by lowercase letters
    import re

    # Pattern to match consecutive uppercase letters followed by lowercase
    # This will match SFTP in SFTPLoader, HTTP in HTTPManager, etc.
    # We need to be more careful about the boundary
    acronym_pattern = r"([A-Z]{2,})(?=[a-z])"
    matches = re.finditer(acronym_pattern, class_name)

    for match in matches:
        acronym = match.group(1)
        # Clean up the acronym by removing any trailing uppercase letters
        # that are followed by lowercase (indicating they're part of the next word)
        clean_acronym = acronym
        for i in range(len(acronym) - 1, 0, -1):
            if (
                acronym[i].isupper()
                and i + 1 < len(class_name)
                and class_name[match.start() + i + 1].islower()
            ):
                clean_acronym = acronym[:i]
                break

        # Check if this cleaned acronym is NOT in our common acronyms list
        if clean_acronym.upper() not in common_acronyms:
            return True

    return False


def convert_to_pascal_case(class_name: str) -> str:
    """Convert a class name to proper PascalCase.

    Examples:
        SFTPLoader -> SftpLoader
        HTTPManager -> HttpManager
        OAuthProvider -> OauthProvider
    """
    # Common acronyms that should remain as-is
    common_acronyms = {
        "SFTP",
        "FTP",
        "FTPS",
        "HTTP",
        "HTTPS",
        "HTTPX",
        "OAUTH",
        "SSH",
        "SSL",
        "TLS",
        "API",
        "URL",
        "URI",
        "JSON",
        "XML",
        "CSV",
        "YAML",
        "ZIP",
        "GZIP",
        "BZIP",
        "LZMA",
        "AWS",
        "CLI",
        "WSGI",
        "KV",
    }

    # Find consecutive uppercase letters (acronyms) and convert them
    result = class_name

    # Handle special cases first
    special_cases = {
        "OAuth": "OAuth",  # Keep OAuth as-is
    }

    for original, replacement in special_cases.items():
        if original in result:
            result = result.replace(original, replacement)

    # Pattern to match consecutive uppercase letters that are followed by lowercase
    # This will match SFTP in SFTPLoader, HTTP in HTTPManager, etc.
    # The lookahead ensures we only convert when there's a lowercase letter after
    acronym_pattern = r"([A-Z]{2,})(?=[a-z])"

    def replace_acronym(match: re.Match[str]) -> str:
        acronym = match.group(1)
        if len(acronym) > 1:
            # Check if this is a common acronym that should remain as-is
            if acronym.upper() in common_acronyms:
                return acronym  # Keep common acronyms unchanged
            # Convert other acronyms to proper case
            return acronym[0] + acronym[1:].lower()
        return acronym

    return re.sub(acronym_pattern, replace_acronym, result)


def main() -> None:
    """Main function to check all Python files in the project."""
    project_root = Path(__file__).parent.parent
    python_files = list(project_root.rglob("*.py"))

    # Exclude certain directories
    exclude_patterns = [
        ".git",
        ".venv",
        "__pycache__",
        ".pytest_cache",
        ".ruff_cache",
        "build",
        "dist",
        "tmp",
    ]

    python_files = [
        f
        for f in python_files
        if not any(pattern in str(f) for pattern in exclude_patterns)
    ]

    all_violations = []

    for file_path in python_files:
        violations = check_class_naming(file_path)
        if violations:
            all_violations.extend([(file_path, *v) for v in violations])

    if all_violations:
        print("Class naming convention violations found:")
        for file_path, line_num, class_name, suggestion in all_violations:
            print(
                f"  {file_path}:{line_num}: Class '{class_name}' should be '{suggestion}'"
            )
        print()
        print("Please rename the classes to follow PascalCase conventions.")
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
