#!/usr/bin/env python3

import argparse
from enum import Enum
from pathlib import Path


class PrintOption(Enum):
    ALL = "all"
    NONE = "none"
    KEEP = "keep"
    RM = "rm"

    def __str__(self):
        return self.value

    def maybe_print(self, should_rm: bool, file: Path):
        if should_rm:
            if self in (PrintOption.ALL, PrintOption.RM):
                print("rm", file)
        else:
            if self in (PrintOption.ALL, PrintOption.KEEP):
                print("keep", file)


def remove(
    root_dir: Path,
    keep_list: list[Path],
    print_opt: PrintOption,
    dry_run: bool = True,
):
    def handle_file(root: Path, name: str, is_dir: bool):
        file = (root / name).absolute()

        should_rm = True
        for guard in keep_list:
            if guard.is_dir() and file.is_relative_to(guard):
                should_rm = False
                break
            elif file == guard:
                should_rm = False
                break

        print_opt.maybe_print(should_rm, file)
        if should_rm and not dry_run:
            try:
                if is_dir:
                    if not any(file.iterdir()):
                        file.rmdir()
                else:
                    file.unlink()
            except Exception as e:
                print(e)

    if dry_run and print_opt == PrintOption.NONE:
        print_opt = PrintOption.ALL

    for root, dirs, files in root_dir.walk(top_down=False):
        for file in files:
            handle_file(root, file, is_dir=False)
        for file in dirs:
            handle_file(root, file, is_dir=True)


def main():
    parser = argparse.ArgumentParser(description="Recursively remove all files not listed in a file")
    parser.add_argument(
        "root",
        help="The root directory to be removed",
    )
    parser.add_argument(
        "keep_list_file",
        help="A file containing a list of files to be preserved",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print files instead of removing them",
    )
    parser.add_argument(
        "--print",
        type=PrintOption,
        choices=list(PrintOption),
        default=PrintOption.NONE,
        help="Print files according to the choice",
    )

    args = parser.parse_args()

    with open(args.keep_list_file) as f:
        keep_list = list(Path(x).absolute() for x in f.read().splitlines())
    remove(Path(args.root), keep_list, print_opt=args.print, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
