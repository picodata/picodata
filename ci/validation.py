import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from xml.dom import minidom


@dataclass
class SvgFile:
    md_path: str = field(init=False)
    svg_path: str = field(init=False)
    doc: minidom.Document = field(init=False)

    @staticmethod
    def new(file: str, md_path: str):
        self = SvgFile()
        self.md_path = md_path
        self.svg_path = file
        self.doc = minidom.parse(file)
        return self

    def file_tags_gen(self):
        """Yield all the xlink:href values in an SVG file."""
        pattern = re.compile(r"(.*)\#(.+)")
        for a in self.doc.getElementsByTagName("a"):
            ref = a.getAttribute("xlink:href")
            if ref:
                for match in pattern.finditer(ref):
                    path = match.group(1)
                    refered_path = (
                        os.path.join(
                            self.md_path,
                            Path(path).with_suffix(".md"),
                        )
                        if path
                        else self.md_path
                    )
                    tag = match.group(2)
                    yield FileTag.new(refered_path, tag).normalized()


@dataclass
class FileTag:
    path: str = field(init=False)
    tag: str = field(init=False)

    @staticmethod
    def new(path: str, header: str):
        self = FileTag()
        self.path = path
        self.tag = header
        return self

    def _trim_bold(self):
        """Parse bold markdown header."""

        # "**HEADER**" is a valid input.
        # We need to trim "**" and lowercase the rest
        # (e.g. "**HEADER**" -> "header") to make mkdocs work.

        pattern = re.compile(r"\*{2}(.+)\*{2}")
        for match in pattern.finditer(self.tag):
            self.tag = match.group(1).lower()
            break

    def _trim_italic(self):
        """Parse italic markdown header."""

        # "__HEADER__" is a valid input.
        # We need to trim "__" and lowercase the rest
        # (e.g. "__HEADER__" -> "header") to make mkdocs work.

        pattern = re.compile(r"\_{2}(.+)\_{2}")
        for match in pattern.finditer(self.tag):
            self.tag = match.group(1).lower()
            break

    def _process_ref(self):
        # "Header {: #tag }" is a valid input.
        # We need to trim everything except the reference, no case
        # change required (e.g. "Header {: #tag }"
        # -> "#tag").

        pattern = re.compile(r".+\{\:\s*\#*(.+)\s*\}")
        for match in pattern.finditer(self.tag):
            tag = match.group(1)
            self.tag = tag
            break

    def normalized(self):
        self.path = os.path.abspath(self.path)
        self.tag = self.tag.strip()
        return self

    def parse(self):
        """Parse markdown header."""
        self._process_ref()
        self._trim_bold()
        self._trim_italic()
        return self.normalized()

    def as_tuple(self):
        return (self.path, self.tag)


@dataclass
class MdFile:
    file: str = field(init=False)

    @staticmethod
    def new(file: str):
        self = MdFile()
        self.file = file
        return self

    def svg_gen(self):
        """Yield all the svg file references in the markdown file."""
        pattern = re.compile(r"!\[.+\]\((\S+\.svg)\)")
        with open(self.file, "r") as f:
            for line in f:
                for match in pattern.finditer(line):
                    yield match.group(1)

    def header_gen(self):
        """Yield and parse all the headers in the markdown file."""
        pattern = re.compile(r"^#+\s+(.+)$")
        with open(self.file, "r") as f:
            for line in f:
                for match in pattern.finditer(line):
                    yield FileTag.new(self.file, match.group(1)).parse()


def file_lookup_gen(root: str, ext: str):
    """Yield all the files with passed extension in a directory tree."""
    for dirpath, dirnames, filenames in os.walk(root):
        for filename in filenames:
            if filename.endswith(ext):
                yield (os.path.join(dirpath, filename))


if __name__ == "__main__":
    has_errors = False
    # As .svg files may contain references to other .md files,
    # we need to materialize all the .md file tags first.
    file_tags = set()
    for file in file_lookup_gen("docs", ".md"):
        md_file = MdFile.new(file)
        for ft in md_file.header_gen():
            file_tags.add(ft.as_tuple())

    for file in file_lookup_gen("docs", ".md"):
        md_file = MdFile.new(file)
        visited = set()
        for svg in md_file.svg_gen():
            if svg in visited:
                continue
            visited.add(svg)
            svg_path = os.path.join(os.path.dirname(file), svg)
            refs = set()
            for fd in SvgFile.new(svg_path, file).file_tags_gen():
                if fd.as_tuple() not in file_tags:
                    refs.add(fd.tag)

            if refs:
                has_errors = True
                print(
                    f"Missing reference(s) {refs} from '{svg}' to '{file}' header(s)."
                )
    if has_errors:
        exit(1)
