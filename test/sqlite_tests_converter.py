import re
import sys
import os


def parse_sqlite_tests(input_text, prefix):
    pattern = re.compile(
        r"(do_test|do_execsql_test)\s+(\S+)\s*\{\s*db eval\s*\{(.*?)\}\s*\}\s*\{(.*?)\}" + r"|"
        r"(do_execsql_test)\s+(\S+)\s*\{\s*(.*?)\s*\}\s*\{(.*?)\}",
        re.DOTALL,
    )

    results = []
    for match in pattern.finditer(input_text):
        if match.group(1):  # do_test or do_execsql_test with db eval
            name = match.group(2)
            sql = match.group(3).strip()
            expected_raw = match.group(4).strip()
        else:  # plain do_execsql_test
            name = match.group(6)
            sql = match.group(7).strip()
            expected_raw = match.group(8).strip()

        expected_values = re.findall(r"(?:[^\s{}]+)", expected_raw)
        formatted_expected = []
        for val in expected_values:
            if re.match(r"^-?\d+(\.\d+)?$", val):
                formatted_expected.append(val)
            else:
                formatted_expected.append(f"'{val}'")

        full_name = f"{prefix}-{name}" if not name.startswith(f"{prefix}-") else name
        results.append({"name": full_name, "sql": sql, "expected": formatted_expected})

    return results


def convert_to_custom_format(tests):
    output_lines = []
    for test in tests:
        output_lines.append(f"-- TEST: {test['name']}")
        output_lines.append("-- SQL:")
        output_lines.extend([line.strip() for line in test["sql"].splitlines()])
        output_lines.append("-- EXPECTED:")
        output_lines.append(", ".join(test["expected"]))
        output_lines.append("")
    return "\n".join(output_lines)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python sqlite_tests_converter.py <file_input_name> <test_name_prefix>")
        sys.exit(1)

    file_input_name = sys.argv[1]
    prefix = sys.argv[2]

    base_name = os.path.splitext(os.path.basename(file_input_name))[0]
    file_output_name = f"{base_name}.sql"

    with open(file_input_name, "r") as infile:
        input_text = infile.read()

    tests = parse_sqlite_tests(input_text, prefix)
    output_text = convert_to_custom_format(tests)

    with open(file_output_name, "w") as outfile:
        outfile.write(output_text)

    print(f"Tests converted and written to {file_output_name}")
