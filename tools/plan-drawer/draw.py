import json
from typing import Any, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass
from enum import Enum
import argparse
import sys

class ArenaType(Enum):
    """Enum for different arena types."""
    ARENA32 = "Arena32"
    ARENA64 = "Arena64"
    ARENA96 = "Arena96"
    ARENA136 = "Arena136"
    ARENA232 = "Arena232"


@dataclass
class NodeId:
    """Represents a node identifier with arena type and offset."""
    offset: int
    arena_type: ArenaType

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'NodeId':
        """Create NodeId from dictionary representation."""
        return cls(
            offset=data["offset"],
            arena_type=ArenaType(data["arena_type"])
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert NodeId to dictionary representation."""
        return {
            "offset": self.offset,
            "arena_type": self.arena_type.value
        }


class RustParser:
    """Parser for converting Rust object string representations to JSON."""

    def parse(self, rust_string: str) -> Dict[str, Any]:
        """
        Convert a Rust object string representation to JSON.

        Args:
            rust_string: The Rust object string to parse

        Returns:
            Dictionary representation of the parsed object
        """
        rust_string = self._clean_input(rust_string)

        try:
            result, _ = self._parse_value(rust_string, 0)
            return result
        except Exception as e:
            print(f"Parsing error: {e}")
            return {"error": "Failed to parse Rust object", "raw": rust_string}

    def _clean_input(self, rust_string: str) -> str:
        """Clean up the input string by removing wrapper quotes."""
        rust_string = rust_string.strip()
        if rust_string.startswith("example = '") and rust_string.endswith("'"):
            rust_string = rust_string[11:-1]
        # Handle single quotes wrapping the entire string
        if rust_string.startswith("'") and rust_string.endswith("'"):
            rust_string = rust_string[1:-1]
        return rust_string

    def _parse_value(self, s: str, pos: int = 0) -> Tuple[Any, int]:
        """Parse a value starting at position pos, return (value, new_position)."""
        pos = self._skip_whitespace(s, pos)

        if pos >= len(s):
            return None, pos

        # Parse different value types - prioritize identifier parsing to catch structs
        parsers = [
            self._try_parse_boolean,
            self._try_parse_none,
            self._try_parse_some,
            self._try_parse_number,
            self._try_parse_string,
            self._try_parse_array,
            self._try_parse_identifier,  # Before object parsing to catch Rust structs
            self._try_parse_collection,  # Handle sets and collections
        ]

        for parser in parsers:
            result = parser(s, pos)
            if result is not None:
                return result

        raise ValueError(f"Unable to parse value at position {pos}: {s[pos:pos + 30]}")

    def _skip_whitespace(self, s: str, pos: int) -> int:
        """Skip whitespace characters starting from position."""
        while pos < len(s) and s[pos].isspace():
            pos += 1
        return pos

    def _try_parse_boolean(self, s: str, pos: int) -> Optional[Tuple[bool, int]]:
        """Try to parse a boolean value."""
        if s[pos:pos + 4] == 'true':
            return True, pos + 4
        elif s[pos:pos + 5] == 'false':
            return False, pos + 5
        return None

    def _try_parse_none(self, s: str, pos: int) -> Optional[Tuple[None, int]]:
        """Try to parse None value."""
        if s[pos:pos + 4] == 'None':
            return None, pos + 4
        return None

    def _try_parse_some(self, s: str, pos: int) -> Optional[Tuple[Any, int]]:
        """Try to parse Some(value) wrapper."""
        if s[pos:pos + 4] != 'Some':
            return None

        pos += 4
        pos = self._skip_whitespace(s, pos)
        if pos >= len(s) or s[pos] != '(':
            return None

        pos += 1  # skip '('
        value, pos = self._parse_value(s, pos)
        pos = self._skip_whitespace(s, pos)
        if pos >= len(s) or s[pos] != ')':
            raise ValueError("Expected ')' after Some value")
        pos += 1  # skip ')'
        return value, pos

    def _try_parse_number(self, s: str, pos: int) -> Optional[Tuple[Union[int, float], int]]:
        """Try to parse a number (int or float)."""
        if pos >= len(s) or not (s[pos].isdigit() or s[pos] == '-'):
            return None

        start = pos
        if s[pos] == '-':
            pos += 1

        has_dot = False
        while pos < len(s) and (s[pos].isdigit() or (s[pos] == '.' and not has_dot)):
            if s[pos] == '.':
                has_dot = True
            pos += 1

        if pos == start or (pos == start + 1 and s[start] == '-'):
            return None

        value_str = s[start:pos]
        try:
            value = float(value_str) if has_dot else int(value_str)
            return value, pos
        except ValueError:
            return None

    def _try_parse_string(self, s: str, pos: int) -> Optional[Tuple[str, int]]:
        """Try to parse a quoted string."""
        if pos >= len(s) or s[pos] != '"':
            return None

        pos += 1
        value_chars = []

        while pos < len(s) and s[pos] != '"':
            if s[pos] == '\\' and pos + 1 < len(s):
                # Handle escaped characters
                pos += 1
                if s[pos] == 'n':
                    value_chars.append('\n')
                elif s[pos] == 't':
                    value_chars.append('\t')
                elif s[pos] == 'r':
                    value_chars.append('\r')
                elif s[pos] == '\\':
                    value_chars.append('\\')
                elif s[pos] == '"':
                    value_chars.append('"')
                else:
                    value_chars.append(s[pos])
            else:
                value_chars.append(s[pos])
            pos += 1

        if pos >= len(s):
            raise ValueError("Unterminated string")

        pos += 1  # skip closing quote
        return ''.join(value_chars), pos

    def _try_parse_array(self, s: str, pos: int) -> Optional[Tuple[List[Any], int]]:
        """Try to parse an array [...]."""
        if pos >= len(s) or s[pos] != '[':
            return None

        pos += 1  # skip '['
        items = []
        pos = self._skip_whitespace(s, pos)

        while pos < len(s) and s[pos] != ']':
            # Parse item
            item, pos = self._parse_value(s, pos)
            items.append(item)

            pos = self._skip_whitespace(s, pos)

            if pos < len(s) and s[pos] == ',':
                pos += 1  # skip comma
                pos = self._skip_whitespace(s, pos)
            elif pos < len(s) and s[pos] != ']':
                raise ValueError(f"Expected ',' or ']' in array at position {pos}")

        if pos >= len(s):
            raise ValueError("Unterminated array")

        pos += 1  # skip ']'
        return items, pos

    def _try_parse_identifier(self, s: str, pos: int) -> Optional[Tuple[Any, int]]:
        """Try to parse an identifier or struct."""
        if pos >= len(s) or not (s[pos].isalpha() or s[pos] == '_'):
            return None

        start = pos
        while pos < len(s) and (s[pos].isalnum() or s[pos] == '_'):
            pos += 1

        identifier = s[start:pos]
        original_pos = pos
        pos = self._skip_whitespace(s, pos)

        if pos < len(s) and s[pos] == '(':
            # Function-like syntax: Identifier(...)
            return self._parse_function_call(s, pos, identifier)

        elif pos < len(s) and s[pos] == '{':
            # Struct syntax: Identifier { ... }
            return self._parse_struct(s, pos, identifier)
        else:
            # Just an identifier
            return identifier, original_pos

    def _parse_function_call(self, s: str, pos: int, identifier: str) -> Tuple[Dict[str, Any], int]:
        """Parse function call syntax: Identifier(args...)"""
        pos += 1  # skip '('

        # Handle empty parentheses
        pos = self._skip_whitespace(s, pos)
        if pos < len(s) and s[pos] == ')':
            pos += 1
            return {"_node_name": identifier}, pos

        # Parse arguments
        args = []
        while pos < len(s) and s[pos] != ')':
            arg, pos = self._parse_value(s, pos)
            args.append(arg)

            pos = self._skip_whitespace(s, pos)
            if pos < len(s) and s[pos] == ',':
                pos += 1
                pos = self._skip_whitespace(s, pos)
            elif pos < len(s) and s[pos] != ')':
                break

        if pos >= len(s) or s[pos] != ')':
            raise ValueError(f"Expected ')' after {identifier} arguments")
        pos += 1  # skip ')'

        # Return as structured object
        result = {"_node_name": identifier}
        if len(args) == 1 and isinstance(args[0], dict) and "_node_name" not in args[0]:
            # Flatten single dict argument
            result.update(args[0])
        elif len(args) == 1:
            if isinstance(args[0], dict) and "_node_name" in args[0]:
                result = args[0]
            else:
                result = args[0]
        elif args:
            result["_args"] = args
        return result, pos

    def _parse_struct(self, s: str, pos: int, identifier: str) -> Tuple[Dict[str, Any], int]:
        """Parse a struct with named fields."""
        pos += 1  # skip '{'
        fields = {"_node_name": identifier}
        pos = self._skip_whitespace(s, pos)

        while pos < len(s) and s[pos] != '}':
            # Parse field name
            field_start = pos
            while pos < len(s) and (s[pos].isalnum() or s[pos] == '_'):
                pos += 1

            if pos == field_start:
                raise ValueError(f"Expected field name in struct at position {pos}")

            field_name = s[field_start:pos]
            pos = self._skip_whitespace(s, pos)

            if pos >= len(s) or s[pos] != ':':
                raise ValueError(f"Expected ':' after field name '{field_name}' at position {pos}")
            pos += 1  # skip ':'
            pos = self._skip_whitespace(s, pos)

            # Parse field value
            field_value, pos = self._parse_value(s, pos)
            fields[field_name] = field_value

            pos = self._skip_whitespace(s, pos)

            if pos < len(s) and s[pos] == ',':
                pos += 1  # skip comma
                pos = self._skip_whitespace(s, pos)
            elif pos < len(s) and s[pos] != '}':
                raise ValueError(f"Expected ',' or '}}' in struct at position {pos}")

        if pos >= len(s):
            raise ValueError("Unterminated struct")

        pos += 1  # skip '}'
        return fields, pos

    def _try_parse_collection(self, s: str, pos: int) -> Optional[Tuple[Any, int]]:
        """Try to parse collections like sets {...} that aren't structs."""
        if pos >= len(s) or s[pos] != '{':
            return None

        # Look back to see if there's an identifier - if so, this should be handled by struct parsing
        temp_pos = pos - 1
        while temp_pos >= 0 and s[temp_pos].isspace():
            temp_pos -= 1

        if temp_pos >= 0 and (s[temp_pos].isalnum() or s[temp_pos] == '_'):
            return None  # This is a struct, let identifier parsing handle it

        # Look ahead to determine if this is a set {item, item} or map {key: value}
        temp_pos = pos + 1
        temp_pos = self._skip_whitespace(s, temp_pos)

        if temp_pos >= len(s):
            return None

        # Try to parse first element and see what follows
        try:
            first_item, next_pos = self._parse_value(s, temp_pos)
            next_pos = self._skip_whitespace(s, next_pos)

            if next_pos < len(s):
                if s[next_pos] == ':':
                    # This looks like {key: value, ...} - parse as map
                    return self._parse_map(s, pos)
                elif s[next_pos] in ',}':
                    # This looks like {item, item, ...} - parse as set
                    return self._parse_set(s, pos)
        except:
            pass

        # Default to trying map first, then set
        try:
            return self._parse_map(s, pos)
        except:
            try:
                return self._parse_set(s, pos)
            except:
                return None

    def _parse_map(self, s: str, pos: int) -> Tuple[Dict[str, Any], int]:
        """Parse a map {key: value, ...}"""
        pos += 1  # skip '{'
        obj = {}
        pos = self._skip_whitespace(s, pos)

        while pos < len(s) and s[pos] != '}':
            # Parse key
            key, pos = self._parse_value(s, pos)
            pos = self._skip_whitespace(s, pos)

            if pos >= len(s) or s[pos] != ':':
                raise ValueError(f"Expected ':' after key in map")
            pos += 1  # skip ':'
            pos = self._skip_whitespace(s, pos)

            # Parse value
            value, pos = self._parse_value(s, pos)
            obj[str(key)] = value

            pos = self._skip_whitespace(s, pos)

            if pos < len(s) and s[pos] == ',':
                pos += 1  # skip comma
                pos = self._skip_whitespace(s, pos)
            elif pos < len(s) and s[pos] != '}':
                raise ValueError(f"Expected ',' or '}}' in map")

        if pos >= len(s):
            raise ValueError("Unterminated map")

        pos += 1  # skip '}'
        return obj, pos

    def _parse_set(self, s: str, pos: int) -> Tuple[Dict[str, List[Any]], int]:
        """Parse a set-like structure {item, item, ...}"""
        pos += 1  # skip '{'
        items = []
        pos = self._skip_whitespace(s, pos)

        while pos < len(s) and s[pos] != '}':
            # Parse item
            item, pos = self._parse_value(s, pos)
            items.append(item)

            pos = self._skip_whitespace(s, pos)

            if pos < len(s) and s[pos] == ',':
                pos += 1  # skip comma
                pos = self._skip_whitespace(s, pos)
            elif pos < len(s) and s[pos] != '}':
                break

        if pos >= len(s):
            raise ValueError("Unterminated set")

        pos += 1  # skip '}'
        return {"_set": items}, pos


class PlanAnalyzer:
    """Analyzer for working with parsed plan data."""

    ARENA_MAPPINGS = {
        ArenaType.ARENA232: ("arena224", "A232"),
        ArenaType.ARENA136: ("arena136", "A136"),
        ArenaType.ARENA96: ("arena96", "A96"),
        ArenaType.ARENA64: ("arena64", "A64"),
        ArenaType.ARENA32: ("arena32", "A32"),
    }

    def __init__(self, plan: Dict[str, Any]):
        """Initialize with parsed plan data."""
        self.plan = plan

    def get_arena_node(self, node_id: Union[Dict[str, Any], NodeId]) -> Dict[str, Any]:
        """Get a node from the appropriate arena."""
        if isinstance(node_id, dict):
            node_id = NodeId.from_dict(node_id)

        arena_key, _ = self.ARENA_MAPPINGS[node_id.arena_type]
        nodes = self.plan["nodes"]

        if arena_key not in nodes:
            raise ValueError(f"Arena {arena_key} not found in plan")

        arena_nodes = nodes[arena_key]
        if node_id.offset >= len(arena_nodes):
            raise ValueError(f"Offset {node_id.offset} out of bounds for {arena_key}")

        node = arena_nodes[node_id.offset]
        if isinstance(node, str):
            return {"_node_name": node}

        return node

    def get_arena_type_short(self, node_id: Union[Dict[str, Any], NodeId]) -> str:
        """Get the short arena type string."""
        if isinstance(node_id, dict):
            node_id = NodeId.from_dict(node_id)

        _, short_name = self.ARENA_MAPPINGS[node_id.arena_type]
        return short_name

    def is_node_reference(self, node: Any) -> bool:
        """Check if a value is a node reference."""
        return (isinstance(node, dict) and
                "_node_name" in node and
                node["_node_name"] == "NodeId")

    def is_node_array(self, node: Any) -> bool:
        """Check if a value is an array of node references."""
        return (isinstance(node, list) and
                (len(node) == 0 or self.is_node_reference(node[0])))

    def get_node_info(self, node_id: Union[Dict[str, Any], NodeId]) -> str:
        """Get formatted information about a node."""
        node = self.get_arena_node(node_id)
        node_name = node.get("_node_name", "Unknown")
        info = self._format_node_info(node, inside=False)
        return f"{node_name}<br>{info}"

    def _format_node_info(self, node: Any, inside: bool = False) -> str:
        """Recursively format node information, skipping NodeId fields."""
        if self.is_node_reference(node):
            return ""

        if isinstance(node, dict):
            return self._format_dict_info(node, inside)
        elif isinstance(node, list):
            return self._format_list_info(node)
        else:
            return str(node)

    def _format_dict_info(self, node: Dict[str, Any], inside: bool) -> str:
        """Format dictionary node information."""
        parts = []

        for key, value in node.items():
            if key == "_node_name":
                continue

            if isinstance(value, (str, int, float, bool, type(None))):
                parts.append(f"{key}:{value}")
            elif not (self.is_node_reference(value) or self.is_node_array(value)):
                formatted_value = self._format_node_info(value, inside=True)
                parts.append(f"{key} [{formatted_value}]")

        separator = "" if inside else "<br>"
        return separator.join(parts)

    def _format_list_info(self, node: List[Any]) -> str:
        """Format list node information."""
        formatted_items = []

        for value in node:
            if isinstance(value, (str, int, float, bool, type(None))):
                formatted_items.append(str(value))
            elif not self.is_node_reference(value):
                formatted_items.append(self._format_node_info(value, inside=True))

        return ", ".join(formatted_items)

    def get_node_references(self, node_id: Union[Dict[str, Any], NodeId]) -> Dict[str, Dict[str, Any]]:
        """Get all node references within a node."""
        node = self.get_arena_node(node_id)
        references = {}
        self._collect_references(node, references)
        return references

    def _collect_references(self, node: Any, references: Dict[str, Any], path: str = "") -> None:
        """Recursively collect node references."""
        if isinstance(node, dict):
            for key, value in node.items():
                current_path = f"{path}.{key}" if path else key

                if self.is_node_reference(value):
                    references[current_path] = value
                else:
                    self._collect_references(value, references, current_path)

        elif isinstance(node, list):
            for i, value in enumerate(node):
                current_path = f"{path}[{i}]" if path else f"[{i}]"

                if self.is_node_reference(value):
                    references[current_path] = value
                else:
                    self._collect_references(value, references, current_path)


class MermaidGenerator:
    """Generator for creating Mermaid diagrams from plan data."""

    def __init__(self, analyzer: PlanAnalyzer):
        self.analyzer = analyzer

    def generate_diagram(self, filename: str = 'f.mmd') -> None:
        """Generate a complete Mermaid diagram file."""
        with open(filename, 'w') as f:
            f.write("graph TD\n")

            # Write top node
            top_id = self.analyzer.plan["top"]
            if top_id:
                self._write_node(f, top_id, is_top=True)

            # Write all arena nodes
            self._write_arena_nodes(f, top_id)

            # Write styles
            self._write_styles(f, top_id)

            # Write references
            self._write_references(f)

    def _write_node(self, f, node_id: Dict[str, Any], is_top: bool = False) -> None:
        """Write a single node definition."""
        offset = node_id["offset"]
        arena_type = node_id["arena_type"]
        short_type = self.analyzer.get_arena_type_short(node_id)

        f.write(f'\t{short_type}_{offset}["{arena_type}({offset})<br>')
        f.write(self.analyzer.get_node_info(node_id))
        f.write('"]\n')

    def _write_arena_nodes(self, f, top_id: Dict[str, Any]) -> None:
        """Write all nodes for each arena."""
        nodes = self.analyzer.plan["nodes"]

        # Write arena64 first for better layout
        if "arena64" in nodes and nodes["arena64"]:
            self._write_arena_section(f, "arena64", nodes["arena64"], top_id)

        # Write other arenas
        for arena_name, arena_nodes in nodes.items():
            if arena_name in ["_node_name", "arena64"] or not arena_nodes:
                continue
            self._write_arena_section(f, arena_name, arena_nodes, top_id)

    def _write_arena_section(self, f, arena_name: str, arena_nodes: List[Any], top_id: Dict[str, Any]) -> None:
        """Write all nodes for a specific arena."""
        f.write(f"\t%% {arena_name.capitalize()} nodes\n")

        for i, _ in enumerate(arena_nodes):
            node_id = {
                "arena_type": arena_name.capitalize(),
                "offset": i
            }

            # Skip top node (will be styled separately)
            if (node_id["offset"] == top_id["offset"] and
                    node_id["arena_type"] == top_id["arena_type"]):
                continue

            self._write_node(f, node_id)

        f.write("\n")

    def _write_styles(self, f, top_id: Dict[str, Any]) -> None:
        """Write CSS classes and styling."""
        style_definitions = [
            "\tclassDef arena32 fill:#f3e5f5,stroke:#4a148c,stroke-width:2px",
            "\tclassDef arena64 fill:#e1f5fe,stroke:#01579b,stroke-width:2px",
            "\tclassDef arena96 fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px",
            "\tclassDef arena136 fill:#fff3e0,stroke:#e65100,stroke-width:2px",
            "\tclassDef arena232 fill:#fce4ec,stroke:#880e4f,stroke-width:2px",
            "\tclassDef topNode fill:#ffeb3b,stroke:#f57f17,stroke-width:3px"
        ]

        for style in style_definitions:
            f.write(f"{style}\n")

        # Style top node
        top_short_type = self.analyzer.get_arena_type_short(top_id)
        f.write(f"\tclass {top_short_type}_{top_id['offset']} topNode\n")

        # Style arena nodes
        self._write_arena_styles(f, top_id)

    def _write_arena_styles(self, f, top_id: Dict[str, Any]) -> None:
        """Write styles for each arena's nodes."""
        nodes = self.analyzer.plan["nodes"]

        for arena_name, arena_nodes in nodes.items():
            if arena_name == "_node_name" or not arena_nodes:
                continue

            # Count nodes excluding top node
            node_count = len(arena_nodes)
            if arena_name.capitalize() == top_id["arena_type"]:
                node_count -= 1

            if node_count == 0:
                continue

            f.write(f"\t%% {arena_name.capitalize()} styles\n")
            f.write("\tclass ")

            node_ids = []
            for i in range(len(arena_nodes)):
                node_id = {
                    "arena_type": arena_name.capitalize(),
                    "offset": i
                }

                # Skip top node
                if (node_id["offset"] == top_id["offset"] and
                        node_id["arena_type"] == top_id["arena_type"]):
                    continue

                short_type = self.analyzer.get_arena_type_short(node_id)
                node_ids.append(f"{short_type}_{i}")

            f.write(",".join(node_ids))
            f.write(f" {arena_name}\n")

    def _write_references(self, f) -> None:
        """Write all reference connections between nodes."""
        nodes = self.analyzer.plan["nodes"]

        for arena_name, arena_nodes in nodes.items():
            if arena_name == "_node_name" or not arena_nodes:
                continue

            f.write(f"\t%% {arena_name.capitalize()} references\n")

            for i in range(len(arena_nodes)):
                node_id = {
                    "arena_type": arena_name.capitalize(),
                    "offset": i
                }

                references = self.analyzer.get_node_references(node_id)
                source_short_type = self.analyzer.get_arena_type_short(node_id)

                for ref_path, target_node in references.items():
                    target_short_type = self.analyzer.get_arena_type_short(target_node)
                    target_offset = target_node["offset"]

                    f.write(f"\t{source_short_type}_{i} -->|\"{ref_path}\"| "
                            f"{target_short_type}_{target_offset}\n")

            f.write("\n")


def main():
    """Main function demonstrating usage."""
    parser = argparse.ArgumentParser(description='Read from file or input')
    parser.add_argument('file', nargs='?', help='Input file (optional)')
    parser.add_argument('-o', '--output', default='output.mmd',
                           help='Output file name (default: output.mmd)')
    args = parser.parse_args()

    if args.file:
        with open(args.file, 'r') as f:
            content = f.read()
    else:
        print("Enter text (Ctrl+D/Ctrl+Z to finish):")
        content = sys.stdin.read()

    content = content.replace("\n", "")
    content = content.replace(" ", "")
    content = content.replace(",}", "}")
    content = content.replace(",)", ")")
    content = content.replace(",]", "]")

    rust_obj_string = content
    # Parse the Rust object
    parser = RustParser()
    result = parser.parse(rust_obj_string)

    # Analyze the plan
    analyzer = PlanAnalyzer(result)

    # Generate Mermaid diagram
    generator = MermaidGenerator(analyzer)
    generator.generate_diagram(args.output)

    print("Parsing and diagram generation completed!")
    print(f"Check {args.output}")


if __name__ == "__main__":
    main()