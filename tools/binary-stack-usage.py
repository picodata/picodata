#!/usr/bin/env python3

# Prints the static stack frame size of each function in an x86_64 or
# aarch64 binary (ELF or Mach-O), largest first. With `--max`, exits with
# code 1 if a function has a larger frame.
#
# The size comes from the disassembly (`objdump -d`). The script walks the
# instructions of each function in address order and keeps the stack depth.
# The frame size is the peak depth. This also works when LLVM puts the
# prologue after an early exit (shrink-wrapping).
#
# The walk also follows the branches in the function. A block after `ret`,
# a trap (`ud2`, `brk`) or a jump starts with the depth at the branches to
# it, not with the depth after the epilogue before it. A block that only a
# backward jump or a jump table reaches starts with the depth at the last
# jump. A cold part of a function (`f.cold` from GCC) runs on the frame of
# the function, so its size is added to the depth at the branches to it.
#
# x86_64: push +8, pop -8, `sub $N,%rsp` +N, `add $N,%rsp` -N, and the
# worst case for `and $-N,%rsp`. The decoder follows %rbp for `leave`,
# `mov %rbp,%rsp` and `lea -N(%rbp),%rsp`. A frame larger than a page uses
# a probe:
#
#     mov %rsp,%r11; sub $N,%r11; L: sub $0x1000,%rsp; ...; jne L   (LLVM)
#     lea -N(%rsp),%r11; L: sub $0x1000,%rsp; ...; jne L            (GCC)
#     mov $N,%eax; call ___chkstk_darwin; sub %rax,%rsp             (macOS)
#
# and then the probe adds N to the depth, not one page. For locals with an
# alignment of a page or more, LLVM probes down to an aligned %r11:
#
#     mov %rsp,%r11; and $-N,%r11; L: sub $0x1000,%rsp; ...; mov %r11,%rsp
#
# The size does not include the return address that `call` pushes.
#
# aarch64: `sub sp, sp, #N` +N, `add sp, sp, #N` -N, and the writeback
# forms `stp x29, x30, [sp, #-N]!` +N and `ldp x29, x30, [sp], #N` -N. The
# decoder also follows the registers that hold sp plus a known offset
# (`mov x29, sp`, `sub x9, sp, #N`) and the constants that `mov`/`movk`
# load, and clears the caller-saved registers at a call. This covers
# `mov sp, x29`, `sub sp, sp, x15, uxtx #4`, `and sp, x9, #-64` and the
# probe:
#
#     sub x9, sp, #N; L: sub sp, sp, #0x1000; ...; cmp sp, x9; b.ne L
#
# With locals of a large alignment, the probe loop ends at an aligned
# register, and sp stops at it:
#
#     sub x9, sp, #N; and x9, x9, #-64; L: sub sp, sp, #0x1000; cmp sp, x9; b.le E; ...; b L; E: mov sp, x9
#
# The size does not include dynamic allocations (alloca, variable length
# arrays in C).
#
# objdump shows one name for the symbols at the same address, e.g. for
# the aliases that LLVM makes of identical functions. `--filter` sees
# only that name.

import argparse
import bisect
import os
import platform
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

FUNC = re.compile(r"^([0-9a-f]+) <(.+)>:$")
# aarch64 mapping symbols mark code and data inside a function, not a new function.
MAPPING_SYMBOL = re.compile(r"^\$[xd](\.\d+)?$")
SECTION = re.compile(r"^Disassembly of section (?:\w+,)?(\S+):$")
# Sections with the stubs that call functions in shared libraries: the
# ELF PLT and the Mach-O stubs. Their entries are not functions.
STUB_SECTIONS = {".plt", ".plt.got", ".plt.sec", ".iplt", "__stubs", "__stub_helper", "__auth_stubs"}

ELF_MAGIC = b"\x7fELF"
EM_X86_64 = 62
EM_AARCH64 = 183
ELF_MACHINES = {EM_X86_64: "x86_64", EM_AARCH64: "aarch64"}

MACHO_MAGIC_64 = b"\xcf\xfa\xed\xfe"
# Universal (fat) binaries, 32-bit and 64-bit headers. They are big-endian.
MACHO_FAT_MAGICS = (b"\xca\xfe\xba\xbe", b"\xca\xfe\xba\xbf")
CPU_TYPE_X86_64 = 0x01000007
CPU_TYPE_ARM64 = 0x0100000C
MACHO_CPU_TYPES = {CPU_TYPE_X86_64: "x86_64", CPU_TYPE_ARM64: "aarch64"}

# The kinds of branches, see Decoder.branch_kind(). RET is also a trap.
COND, JUMP, INDIRECT, RET = range(4)
# A part of a function that GCC moves out of the hot path, e.g. `f.cold`.
# Demangled C++ names: `f() (.cold)` (llvm-objdump), `f() [clone .cold]` (GNU objdump).
COLD_PART = re.compile(r"\.cold(\.\d+)?[)\]]?$")
# The offset in a branch target `<f+0x10>`. A demangled name can contain `+`: `operator+`.
OFFSET = re.compile(r"\+0x[0-9a-f]+$")


@dataclass
class Frame:
    size: int
    address: int
    name: str
    # The address of a cold part of this function -> the depth at the
    # branches to it. The cold part runs on the frame of this function.
    cold: dict[int, int] = field(default_factory=dict)


def signed(v: int) -> int:
    """Interprets a 64-bit value as signed."""
    return v - (1 << 64) if v >= 1 << 63 else v


def branch_target(raw: str) -> tuple[int | None, str | None]:
    """The target address of a direct branch, and the symbol that objdump shows for it.

    GNU objdump prints `jne 401234 <f+0x34>`, llvm-objdump prints
    `jne 0x401234 <f+0x34>`, and aarch64 adds operands before the target:
    `cbz x0, 401234 <f+0x34>`.
    """
    j = raw.find(" <")
    head = raw if j < 0 else raw[:j]
    token = head.replace(",", " ").split()[-1]
    try:
        address = int(token, 16)
    except ValueError:
        address = None
    # A demangled name can contain `>`, e.g. `<<sql_ir::ir::Plan>::optimize+0x10>`.
    symbol = None if j < 0 else raw[j + 2 : raw.rfind(">")]
    return address, symbol


class Decoder:
    """Keeps the stack depth of one function and its peak.

    The probe loop of a dynamic allocation (alloca) also moves sp down by a
    page, and compares sp with a register of unknown value before or after
    it:

        L: sub $0x1000,%rsp; orb $0x0,0xff8(%rsp); cmp %rax,%rsp; jne L
        L: cmp %rdx,%rsp; je E; sub $0x1000,%rsp; orb $0x0,0xff8(%rsp); jmp L
        L: sub sp, sp, #0x1000; cmp sp, x8; b.le E; ldr xzr, [sp]; b L

    That page is a part of the dynamic allocation, not of the frame.
    """

    # The compare is at most this number of instructions from the page.
    PROBE_WINDOW = 3

    def __init__(self, name: str):
        # The name of the function as objdump prints it in branch targets.
        self.name = name
        self.depth = 0
        self.peak = 0
        # The index of the current instruction in the function. The caller
        # increments it for each instruction, also for the ones that it does
        # not give to step() because relevant() returned False.
        self.n = 0
        # (size, peak before, index) of the last page that can be a part of
        # a dynamic allocation.
        self.page = None
        # The index of the last dynamic probe compare.
        self.probe_n = -self.PROBE_WINDOW - 1
        # Branch target address -> the largest depth at the branches to it.
        self.targets = {}
        # True after an instruction that does not continue to the next one.
        self.dead = False
        # True after `ret` or a jump until a branch target gives the depth.
        self.unknown = False
        # The depth at the last jump in the function: for a block after
        # `ret` or a jump that no forward branch reaches. After a jump
        # table, the depth at the table jump.
        self.fallback = None
        self.table = False
        # The address of a cold part -> the largest depth at the branches to it.
        self.cold = {}

    def step(self, insn: str):
        page = self.page
        self.decode(insn)
        if self.is_dynamic_probe(insn):
            self.probe_n = self.n
            # decode() did not record a new page, and the old one is near.
            if page is not None and self.page is page:
                size, peak, n = page
                self.page = None
                if self.n - n <= self.PROBE_WINDOW:
                    self.depth -= size
                    self.peak = max(peak, self.depth)
                    return
        self.peak = max(self.peak, self.depth)

    def resume(self, address: int):
        """Sets the depth after `ret` or a jump, at the first instruction with a known depth.

        The first instruction after `ret` is often alignment padding that no
        branch reaches. Until a branch target comes, the depth is a guess:
        the depth at the last jump, else the depth after the epilogue.
        """
        depth = self.targets.get(address)
        if depth is not None:
            self.depth, self.dead, self.unknown = depth, False, False
        elif self.dead:
            self.dead, self.unknown = False, True
            if self.fallback is not None:
                self.depth = self.fallback

    def branch(self, kind: int, raw: str):
        if kind == RET:
            self.dead = True
            return
        if kind == INDIRECT:
            # A jump table: its cases follow the jump and start at this
            # depth. A case ends with a jump to a common block, often with
            # its call arguments still on the stack, so the jumps at the
            # end of the cases must not change this guess.
            self.fallback = self.depth
            self.table = True
            self.dead = True
            return
        address, symbol = branch_target(raw)
        # A branch to another function is a tail call. Without a symbol,
        # assume that the target is in this function.
        internal = symbol is None or symbol == self.name or symbol.startswith(self.name + "+")
        # A cold part also jumps inside itself: `jne <f.cold+0x10>`.
        if internal and address is not None:
            self.targets[address] = max(self.targets.get(address, self.depth), self.depth)
        elif symbol is not None and address is not None and COLD_PART.search(OFFSET.sub("", symbol)):
            self.cold[address] = max(self.cold.get(address, self.depth), self.depth)
        if kind == JUMP:
            if internal and not self.table:
                self.fallback = self.depth
            self.dead = True

    def maybe_dynamic_page(self, size: int) -> bool:
        """Checks a page that a dynamic probe loop can move sp by. Call before the depth changes.

        Returns True if a dynamic probe compare came just before: then the
        caller must not add the page. Otherwise, a compare that comes just
        after removes the page.
        """
        if self.n - self.probe_n <= self.PROBE_WINDOW:
            return True
        self.page = (size, self.peak, self.n)
        return False

    def branch_kind(self, raw: str) -> int | None:
        """COND, JUMP, INDIRECT or RET if the raw instruction text is a branch, else None.

        Calls are not branches. A trap (`ud2`, `brk`) is RET: LLVM puts it
        after a call that does not return, e.g. after a Rust panic.
        """
        raise NotImplementedError

    def relevant(self, raw: str) -> bool:
        """A quick check on the raw instruction text: False if step() cannot change the state.

        Most instructions do not touch the stack. The regular expressions in
        step() are slow, so this check skips them first.
        """
        raise NotImplementedError

    def decode(self, insn: str):
        raise NotImplementedError

    def is_dynamic_probe(self, insn: str) -> bool:
        """Returns True if insn compares sp with a register of unknown value."""
        raise NotImplementedError


class X86_64(Decoder):
    """Decoder for the AT&T syntax of GNU objdump and llvm-objdump."""

    # GNU objdump prints `sub $0x10,%rsp` and `add $0xffffffffffffff80,%rsp`,
    # llvm-objdump prints `subq $0x10, %rsp` and `addq $-0x80, %rsp`, and
    # before LLVM 15 `subq $16, %rsp`. The optional `q` suffix, sign and
    # decimal numbers cover all; normalize() removes the other differences.
    NUM = r"0x[0-9a-f]+|\d+"
    IMM = rf"\$(-?(?:{NUM}))"
    SUB_RSP = re.compile(rf"^subq? {IMM},%rsp$")
    ADD_RSP = re.compile(rf"^addq? {IMM},%rsp$")
    LEA_RSP = re.compile(rf"^leaq? (-?(?:{NUM}))\(%rsp\),%rsp$")
    MOV_R11 = re.compile(r"^movq? %rsp,%r11$")
    SUB_R11 = re.compile(rf"^subq? {IMM},%r11$")
    LEA_R11 = re.compile(rf"^leaq? -({NUM})\(%rsp\),%r11$")
    MOV_RAX = re.compile(rf"^mov[lq]? {IMM},%[er]ax$")
    SUB_RSP_RAX = re.compile(r"^subq? %rax,%rsp$")
    # Realignment for locals with a large alignment: `and $-0x40,%rsp`.
    AND_RSP = re.compile(rf"^andq? {IMM},%rsp$")
    AND_R11 = re.compile(rf"^andq? {IMM},%r11$")
    MOV_R11_RSP = re.compile(r"^movq? %r11,%rsp$")
    # `push`, `pushq`, `pushf`, `pushfq`, and `pushw` with 2 bytes; not `popcnt`.
    PUSH_POP = re.compile(r"^(push|pop)f?([wq])?(?: |$)")
    CMP_RSP = re.compile(r"^cmpq? (?:%(r\w+),%rsp|%rsp,%(r\w+))$")
    # The frame pointer: `mov %rsp,%rbp` and `lea 0x10(%rsp),%rbp` set it,
    # `leave`, `mov %rbp,%rsp` and `lea -0x8(%rbp),%rsp` restore sp from it.
    MOV_RSP_RBP = re.compile(r"^movq? %rsp,%rbp$")
    LEA_RSP_RBP = re.compile(rf"^leaq? (-?(?:{NUM}))?\(%rsp\),%rbp$")
    MOV_RBP_RSP = re.compile(r"^movq? %rbp,%rsp$")
    LEA_RBP_RSP = re.compile(rf"^leaq? (-?(?:{NUM}))?\(%rbp\),%rsp$")
    WRITES_RBP = re.compile(r",%rbp$|^popq? %rbp$")
    PAGE = 0x1000
    # Prefixes before a branch: `repz ret`, `bnd jmp`, `notrack jmp *%rax`.
    PREFIXES = ("rep ", "repz ", "bnd ", "notrack ")
    TRAPS = ("ud2", "ud1", "ud0", "int3", "hlt")

    def __init__(self, name: str):
        super().__init__(name)
        # The probe size in %r11, and the index of `mov %rsp,%r11`.
        self.r11 = None
        self.r11_n = -1
        self.mov_r11_n = -2
        # The depth at `mov %rsp,%r11`, and the depth of an aligned %r11.
        self.r11_start = None
        self.r11_end = None
        # A constant in %rax, the index of the `mov` that loads it, and the
        # index of a call right after it that can be ___chkstk_darwin.
        self.rax = None
        self.rax_n = -2
        self.chkstk_n = -2
        # The depth at which sp has the value of %rbp.
        self.rbp = None

    @staticmethod
    def normalize(insn: str) -> str:
        """Removes the comment and the spaces after commas, and makes all other whitespace one space."""
        return " ".join(insn.split("#", 1)[0].split()).replace(", ", ",")

    @staticmethod
    def imm(m: re.Match) -> int:
        return signed(int(m.group(1) or "0", 0) & ((1 << 64) - 1))

    @staticmethod
    def aligned(depth: int, v: int) -> int:
        """The depth of sp at depth `depth` after `and $v` (v < 0), in the worst case.

        sp goes down by up to the alignment minus the alignment that sp
        already has. sp is 16-aligned before a call, so at depth d (without
        the return address) it is 16-aligned if d + 8 is a multiple of 16,
        else 8-aligned.
        """
        known = 16 if (depth + 8) % 16 == 0 else 8
        return depth + max(-v - known, 0)

    def branch_kind(self, raw: str) -> int | None:
        c = raw[0]
        if c in "brn" and raw.startswith(self.PREFIXES):
            raw = raw.split(None, 1)[1]
            c = raw[0]
        if c == "j":
            if raw.startswith("jmp"):
                return INDIRECT if "*" in raw else JUMP
            return COND
        if (c == "r" and raw.startswith("ret")) or (c in "uih" and raw.startswith(self.TRAPS)):
            return RET
        return None

    def relevant(self, raw: str) -> bool:
        if raw.startswith(("push", "pop", "leave")) or "%r11" in raw:
            return True
        # %rsp or %rbp as an operand, not only as an address like `0x8(%rsp)`.
        if "%rsp" in raw and raw.count("%rsp") > raw.count("(%rsp"):
            return True
        if "%rbp" in raw and raw.count("%rbp") > raw.count("(%rbp"):
            return True
        # A constant to %rax, and the call right after it.
        if "$" in raw and ("%eax" in raw or "%rax" in raw):
            return True
        return self.rax_n == self.n - 1 and raw.startswith("call")

    def decode(self, insn: str):
        if m := self.PUSH_POP.match(insn):
            size = 2 if m.group(2) == "w" else 8
            self.depth += size if m.group(1) == "push" else -size
            if insn == "pop %rbp" or insn == "popq %rbp":
                self.rbp = None
        elif self.MOV_RSP_RBP.match(insn):
            self.rbp = self.depth
        elif m := self.LEA_RSP_RBP.match(insn):
            self.rbp = self.depth - self.imm(m)
        elif insn == "leave" or insn == "leaveq":
            # leave = mov %rbp,%rsp; pop %rbp.
            if self.rbp is not None:
                self.depth = self.rbp
            self.depth -= 8
            self.rbp = None
        elif self.MOV_RBP_RSP.match(insn):
            if self.rbp is not None:
                self.depth = self.rbp
        elif m := self.LEA_RBP_RSP.match(insn):
            if self.rbp is not None:
                self.depth = self.rbp - self.imm(m)
        elif m := self.AND_RSP.match(insn):
            if (v := self.imm(m)) < 0:
                self.depth = self.aligned(self.depth, v)
        elif self.WRITES_RBP.search(insn):
            self.rbp = None
        elif self.MOV_R11.match(insn):
            self.mov_r11_n, self.r11_start, self.r11_end = self.n, self.depth, None
        elif m := self.AND_R11.match(insn):
            if self.mov_r11_n == self.n - 1 and (v := self.imm(m)) < 0:
                self.r11_end = self.aligned(self.r11_start, v)
        elif self.MOV_R11_RSP.match(insn):
            if self.r11_end is not None:
                # The probe loop moves sp by whole pages until sp passes %r11.
                pages = -(-(self.r11_end - self.r11_start) // self.PAGE)
                self.peak = max(self.peak, self.r11_start + pages * self.PAGE)
                self.depth, self.r11_end = self.r11_end, None
        elif m := self.SUB_R11.match(insn):
            # Only right after `mov %rsp,%r11`: %r11 is also a scratch register.
            if self.mov_r11_n == self.n - 1:
                self.r11, self.r11_n = self.imm(m), self.n
        elif m := self.LEA_R11.match(insn):
            self.r11, self.r11_n = self.imm(m), self.n
        elif m := self.SUB_RSP.match(insn):
            v = self.imm(m)
            # The probe loop starts right after %r11 gets its end address.
            if self.r11 is not None and v == self.PAGE and self.r11_n == self.n - 1:
                self.depth += self.r11
                self.r11 = None
            elif v != self.PAGE or not self.maybe_dynamic_page(v):
                self.depth += v
        elif m := self.ADD_RSP.match(insn):
            self.depth -= self.imm(m)
        elif m := self.LEA_RSP.match(insn):
            self.depth -= self.imm(m)
        elif m := self.MOV_RAX.match(insn):
            self.rax, self.rax_n = self.imm(m), self.n
        elif self.SUB_RSP_RAX.match(insn):
            # Only `mov $N,%eax; sub %rax,%rsp` and `mov $N,%eax; call
            # ___chkstk_darwin; sub %rax,%rsp`. Any other %rax is a dynamic
            # allocation.
            if self.rax_n == self.n - 1 or (self.chkstk_n == self.n - 1 and self.rax_n == self.n - 2):
                self.depth += self.rax
        elif insn.startswith("call") and self.rax_n == self.n - 1:
            # An object file does not show the name of the callee. A call to
            # another known function returns a new value in %rax.
            _, symbol = branch_target(insn)
            if symbol is None or "chkstk" in symbol or symbol.startswith(self.name + "+"):
                self.chkstk_n = self.n

    def is_dynamic_probe(self, insn: str) -> bool:
        # The static probe loop compares with %r11, which decode() already used.
        m = self.CMP_RSP.match(insn)
        return m is not None and (m.group(1) or m.group(2)) != "r11"


class AArch64(Decoder):
    """Decoder for the syntax of GNU objdump and llvm-objdump."""

    # An immediate: `#0x10`, `#16`, `#-0x20`, with an optional `,lsl #12`.
    IMM = r"#(-?(?:0x[0-9a-f]+|\d+))(?:,lsl #(\d+))?"
    REG = r"(x\d+)"
    # A register operand with an optional extend or shift, e.g. `x15,uxtx #4`.
    REG_EXT = r"([xw]\d+)(?:,(?:uxtx|uxtw|lsl)(?: #(\d+))?)?"

    SUB_SP_IMM = re.compile(rf"^sub sp,sp,{IMM}$")
    ADD_SP_IMM = re.compile(rf"^add sp,sp,{IMM}$")
    SUB_SP_REG = re.compile(rf"^sub sp,sp,{REG_EXT}$")
    ADD_SP_REG = re.compile(rf"^add sp,sp,{REG_EXT}$")
    # Writeback addressing changes sp by the offset: `[sp,#-16]!` and `[sp],#16`.
    PRE_INDEX = re.compile(rf"\[sp,{IMM}\]!$")
    POST_INDEX = re.compile(rf"\[sp\],{IMM}$")
    # Registers that get sp plus an offset.
    MOV_FROM_SP = re.compile(rf"^mov {REG},sp$")
    ADD_FROM_SP = re.compile(rf"^add {REG},sp,{IMM}$")
    SUB_FROM_SP = re.compile(rf"^sub {REG},sp,{IMM}$")
    SUB_FROM_SP_REG = re.compile(rf"^sub {REG},sp,{REG_EXT}$")
    SUB_SELF = re.compile(rf"^sub {REG},\1,{IMM}$")
    # sp gets a register plus an offset.
    MOV_TO_SP = re.compile(rf"^mov sp,{REG}$")
    ADD_TO_SP = re.compile(rf"^add sp,{REG},{IMM}$")
    SUB_TO_SP = re.compile(rf"^sub sp,{REG},{IMM}$")
    CMP_SP = re.compile(rf"^cmp sp,{REG}$")
    # Realignment for locals with a large alignment: `sub x9, sp, #N; and sp, x9, #-64`,
    # or `and x9, x9, #-64` before a probe loop that ends at x9.
    AND_SP = re.compile(rf"^and sp,{REG},#(-?(?:0x[0-9a-f]+|\d+))$")
    AND_SELF = re.compile(rf"^and {REG},\1,#(-?(?:0x[0-9a-f]+|\d+))$")
    # The probe interval of LLVM (4 KiB) and GCC (64 KiB).
    PROBE_PAGES = (0x1000, 0x10000)
    # The probe loop starts at most this number of instructions after the
    # probe register gets its end address.
    PROBE_START = 2
    # A call can change these registers. ___chkstk_darwin keeps them.
    CALLER_SAVED = [f"x{i}" for i in range(19)] + ["x30"]
    # Base register writeback: `[x8, #16]!` and `[x8], #8`.
    WRITEBACK = re.compile(r"\[(x\d+)(?:,[^\]]*)?\]!|\[(x\d+)\],")
    TRAPS = ("brk", "udf", "hlt")
    # Constants. A write to a w register sets the upper 32 bits of the x register to zero.
    MOV_IMM = re.compile(rf"^mov(?:z)? ([xw]\d+),{IMM}$")
    MOVK = re.compile(rf"^movk ([xw]\d+),{IMM}$")
    # Instructions whose register operands are only read.
    NO_DEST = {
        "cmp", "cmn", "tst", "ccmp", "ccmn", "cbz", "cbnz", "tbz", "tbnz", "b", "bl", "br", "blr",
        "braa", "brab", "braaz", "brabz", "blraa", "blrab", "blraaz", "blrabz", "ret", "retaa", "retab",
        "prfm", "prfum", "msr", "sys", "dc", "ic", "at", "tlbi", "hint", "nop",
    }  # fmt: skip
    # Exclusive stores write a status register as the first operand.
    STORE_EXCLUSIVE = {"stxr", "stlxr", "stxrb", "stlxrb", "stxrh", "stlxrh", "stxp", "stlxp"}
    # Pair loads (and casp) write the first two operands.
    PAIR_DEST = ("ldp", "ldnp", "ldxp", "ldaxp", "casp")
    # Atomic memory operations `ldadd xs, xt, [xn]` write the second operand.
    ATOMIC = re.compile(r"^(?:ld(?:add|clr|eor|set|smax|smin|umax|umin)|swp)[a-z]*$")

    def __init__(self, name: str):
        super().__init__(name)
        # Register -> the depth at which sp has the value of the register.
        self.rel = {}
        # Register -> a constant.
        self.const = {}
        # The register with the end address of a probe loop, and the index
        # of the instruction that set it.
        self.probe = None
        self.probe_set_n = -self.PROBE_START - 1
        # (register, depth) at which the last probe loop stopped sp.
        self.probe_end = None

    @staticmethod
    def normalize(insn: str) -> str:
        """Removes the comment and the spaces after commas, and makes all other whitespace one space.

        A comment starts with `//`, or with `;` in the Apple syntax that llvm-objdump uses for Mach-O.
        """
        return " ".join(re.split(r"//|;", insn, maxsplit=1)[0].split()).replace(", ", ",")

    @staticmethod
    def imm(m: re.Match, group: int = 1) -> int:
        v = int(m.group(group), 0)
        shift = m.group(group + 1)
        return v << int(shift) if shift else v

    @staticmethod
    def alignment(mask: str) -> int:
        """The alignment of an `and` mask, e.g. 64 for #0xffffffffffffffc0; not positive for other masks."""
        return -signed(int(mask, 0) & ((1 << 64) - 1))

    @staticmethod
    def x(reg: str) -> str:
        """The x register for a w or x register name."""
        return "x" + reg[1:]

    def reg_value(self, m: re.Match, group: int = 1) -> int | None:
        """The value of a constant register operand with its shift, or None."""
        reg = m.group(group)
        c = self.const.get(self.x(reg))
        if c is None:
            return None
        if reg[0] == "w":
            c &= 0xFFFFFFFF
        return c << int(m.group(group + 1) or 0)

    @classmethod
    def dests(cls, raw: str) -> list[str]:
        """The x names of the registers that an instruction writes."""
        parts = raw.split(None, 1)
        if len(parts) < 2:
            return []
        mnemonic = parts[0]
        if mnemonic in cls.NO_DEST or mnemonic.startswith("b."):
            return []
        ops = [o.strip() for o in parts[1].split(",", 2)]
        if mnemonic.startswith("st") and mnemonic not in cls.STORE_EXCLUSIVE:
            regs = []
        elif mnemonic.startswith(cls.PAIR_DEST):
            regs = ops[:2]
        elif cls.ATOMIC.match(mnemonic):
            regs = ops[1:2]
        else:
            regs = ops[:1]
        # A load or store with writeback also changes its base register.
        if (m := cls.WRITEBACK.search(parts[1])) is not None:
            regs.append(m.group(1) or m.group(2))
        return [cls.x(r) for r in regs if r[:1] in ("x", "w") and r[1:].isdigit()]

    def probe_target(self) -> int | None:
        """The depth at the end of the static probe loop, if the loop can start here."""
        if self.probe is None or self.n - self.probe_set_n > self.PROBE_START:
            return None
        return self.rel.get(self.probe)

    def set_probe(self, reg: str | None):
        self.probe, self.probe_set_n = reg, self.n

    def grow(self, v: int):
        """Moves sp down by v. In a probe loop, sp goes down to the probe register."""
        target = self.probe_target()
        if target is not None and v in self.PROBE_PAGES and self.depth + v <= target:
            # The loop moves sp by whole pages until sp passes the register,
            # then `mov sp, x9` moves sp up to it.
            pages = -(-(target - self.depth) // v)
            self.peak = max(self.peak, self.depth + pages * v)
            self.depth = target
            self.probe_end, self.probe = (self.probe, target), None
        else:
            self.depth += v

    def page_end(self, reg: str) -> int | None:
        """The depth of reg if it ended the last probe loop, and the last page, just before, moved sp below it."""
        d = self.rel.get(reg)
        if self.probe_end != (reg, d) or self.page is None or self.n - self.page[2] > self.PROBE_WINDOW:
            return None
        return d if self.depth - self.page[0] <= d < self.depth else None

    def is_dynamic_probe(self, insn: str) -> bool:
        m = self.CMP_SP.match(insn)
        return m is not None and m.group(1) not in self.rel

    def branch_kind(self, raw: str) -> int | None:
        c = raw[0]
        if c in "buh" and raw.startswith(self.TRAPS):
            return RET
        if c == "b":
            mnemonic = raw.split(None, 1)[0]
            if mnemonic == "b":
                return JUMP
            if mnemonic.startswith("b."):
                return COND
            if mnemonic.startswith("br"):
                return INDIRECT
            return None
        if (c == "c" and raw.startswith(("cbz", "cbnz"))) or (c == "t" and raw.startswith(("tbz", "tbnz"))):
            return COND
        if c == "r" and raw.startswith("ret"):
            return RET
        return None

    def relevant(self, raw: str) -> bool:
        # sp as an operand, not only as an address like `[sp, #8]`, or a
        # writeback to sp. The `<symbol>` of a call can also contain "sp".
        if "sp" in raw:
            j = raw.find(" <")
            head = raw if j < 0 else raw[:j]
            if head.count("sp") > head.count("[sp") or "]!" in head or "[sp]," in head:
                return True
        # A constant to a register.
        if raw.startswith("mov") and "#" in raw:
            return True
        # Any write to a register with a known value makes the value unknown.
        # A call can write all caller-saved registers.
        if self.rel or self.const:
            return raw.startswith("bl") or any(r in self.rel or r in self.const for r in self.dests(raw))
        return False

    def decode(self, insn: str):
        if m := self.SUB_SP_IMM.match(insn):
            v = self.imm(m)
            if self.probe_target() is None and v in self.PROBE_PAGES and self.maybe_dynamic_page(v):
                return
            self.grow(v)
        elif m := self.ADD_SP_IMM.match(insn):
            self.depth -= self.imm(m)
        elif m := self.SUB_SP_REG.match(insn):
            # A register that is not a known constant is a dynamic allocation.
            if (c := self.reg_value(m)) is not None:
                self.grow(c)
        elif m := self.ADD_SP_REG.match(insn):
            if (c := self.reg_value(m)) is not None:
                self.depth -= c
        elif m := self.MOV_TO_SP.match(insn):
            # The end of a probe loop.
            self.probe_end = None
            if (d := self.rel.get(m.group(1))) is not None:
                self.depth = d
        elif m := self.ADD_TO_SP.match(insn):
            if (d := self.rel.get(m.group(1))) is not None:
                self.depth = d - self.imm(m, 2)
        elif m := self.SUB_TO_SP.match(insn):
            if (d := self.rel.get(m.group(1))) is not None:
                self.depth = d + self.imm(m, 2)
        elif m := self.AND_SP.match(insn):
            # sp goes down by up to the alignment minus 16: sp is 16-aligned.
            if (d := self.rel.get(m.group(1))) is not None:
                self.depth = d + max(self.alignment(m.group(2)) - 16, 0)
        elif (m := self.CMP_SP.match(insn)) and (d := self.page_end(m.group(1))) is not None:
            # A rotated probe loop moves sp by one more page before this
            # compare. grow() has already counted the pages of the loop.
            self.depth, self.peak, self.page, self.probe_end = d, self.page[1], None, None
        else:
            self.track(insn)
            if m := self.PRE_INDEX.search(insn):
                self.depth -= self.imm(m)
            elif m := self.POST_INDEX.search(insn):
                self.depth -= self.imm(m)

    def track(self, insn: str):
        """Updates the registers that hold sp plus an offset, and the constants."""
        if insn.startswith("bl"):
            # Only ___chkstk_darwin keeps the caller-saved registers. An object
            # file does not show the name of the callee: then keep x15, the
            # argument of ___chkstk_darwin.
            # `blr x8` and the other indirect calls have no symbol: "".
            symbol = branch_target(insn)[1] if insn.startswith("bl ") else ""
            if symbol is None or "chkstk" not in symbol:
                unknown = symbol is None or symbol == self.name or symbol.startswith(self.name + "+")
                for r in self.CALLER_SAVED:
                    if not (unknown and r == "x15"):
                        self.set(r)
        elif m := self.MOV_FROM_SP.match(insn):
            self.set(m.group(1), rel=self.depth)
        elif m := self.ADD_FROM_SP.match(insn):
            self.set(m.group(1), rel=self.depth - self.imm(m, 2))
        elif m := self.SUB_FROM_SP.match(insn):
            self.set(m.group(1), rel=self.depth + self.imm(m, 2))
            self.set_probe(m.group(1))
        elif m := self.SUB_FROM_SP_REG.match(insn):
            c = self.reg_value(m, 2)
            self.set(m.group(1), rel=None if c is None else self.depth + c)
            self.set_probe(m.group(1) if c is not None else None)
        elif (m := self.SUB_SELF.match(insn)) and m.group(1) in self.rel:
            self.rel[m.group(1)] += self.imm(m, 2)
            if self.probe == m.group(1):
                self.set_probe(self.probe)
        elif (m := self.AND_SELF.match(insn)) and m.group(1) in self.rel and self.alignment(m.group(2)) > 0:
            # The register goes down by up to the alignment minus 16, as sp in `and sp, x9, #-64`.
            self.rel[m.group(1)] += max(self.alignment(m.group(2)) - 16, 0)
            if self.probe == m.group(1):
                self.set_probe(self.probe)
        elif m := self.MOV_IMM.match(insn):
            c = self.imm(m, 2)
            self.set(self.x(m.group(1)), const=c & 0xFFFFFFFF if m.group(1)[0] == "w" else c)
        elif m := self.MOVK.match(insn):
            # movk replaces 16 bits at the shift and keeps the other bits.
            r = self.x(m.group(1))
            if r in self.const:
                shift = int(m.group(3) or 0)
                self.const[r] = (self.const[r] & ~(0xFFFF << shift)) | (int(m.group(2), 0) << shift)
                if m.group(1)[0] == "w":
                    self.const[r] &= 0xFFFFFFFF
        else:
            # Any other write makes the value of the register unknown.
            for r in self.dests(insn):
                self.set(r)

    def set(self, reg: str, rel: int | None = None, const: int | None = None):
        self.rel.pop(reg, None)
        self.const.pop(reg, None)
        if rel is not None:
            self.rel[reg] = rel
        if const is not None:
            self.const[reg] = const
        if self.probe == reg and rel is None:
            self.probe = None
        if self.probe_end is not None and self.probe_end[0] == reg:
            self.probe_end = None


DECODERS = {"x86_64": X86_64, "aarch64": AArch64}
HOST_ARCH = {"x86_64": "x86_64", "amd64": "x86_64", "aarch64": "aarch64", "arm64": "aarch64"}.get(
    platform.machine().lower()
)


def check_binary(path: Path) -> tuple[str, str]:
    """Returns the format ("elf" or "macho") and the architecture of the file."""
    try:
        with open(path, "rb") as f:
            header = f.read(20)
    except OSError as e:
        sys.exit(f"{path}: {e.strerror}")

    if header[:4] == ELF_MAGIC and len(header) >= 20:
        # e_machine is a 16-bit field at offset 18, in the byte order of e_ident[EI_DATA].
        byteorder = "little" if header[5] == 1 else "big"
        machine = int.from_bytes(header[18:20], byteorder)
        if machine not in ELF_MACHINES:
            sys.exit(f"{path}: unsupported ELF machine {machine}, only x86_64 and aarch64 are supported")
        return "elf", ELF_MACHINES[machine]

    if header[:4] == MACHO_MAGIC_64 and len(header) >= 8:
        cputype = int.from_bytes(header[4:8], "little")
        if cputype not in MACHO_CPU_TYPES:
            sys.exit(f"{path}: unsupported Mach-O CPU type 0x{cputype:x}, only x86_64 and arm64 are supported")
        return "macho", MACHO_CPU_TYPES[cputype]

    if header[:4] in MACHO_FAT_MAGICS:
        sys.exit(f"{path}: universal binary, extract one architecture with `lipo -thin arm64` or `lipo -thin x86_64`")

    sys.exit(f"{path}: not an ELF or a 64-bit Mach-O file")


def default_objdump(fmt: str, arch: str) -> str:
    """The GNU objdump of the host can usually read only ELF files of the host architecture."""
    if fmt == "elf" and arch == HOST_ARCH:
        return "objdump"
    candidates = ["llvm-objdump"]
    if fmt == "elf":
        candidates.insert(0, f"{arch}-linux-gnu-objdump")
    else:
        # On macOS, objdump is llvm-objdump.
        candidates.append("objdump")
    for candidate in candidates:
        if shutil.which(candidate):
            return candidate
    sys.exit(f"no objdump for {fmt} {arch}: install one of {', '.join(candidates)}, or use --objdump")


def frames(path: Path, fmt: str, arch: str, objdump: str, demangle: bool):
    # Without `-j`, objdump reads all code sections. An object file can keep
    # each function in its own section, e.g. `.text.<name>`.
    cmd = [objdump, "-d", "--no-show-raw-insn", str(path)]
    if demangle:
        cmd.insert(2, "-C")
    decoder_type = DECODERS[arch]
    # The section headers must be in English: see SECTION.
    env = dict(os.environ, LC_ALL="C")
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True, bufsize=1 << 20, env=env)
    assert proc.stdout is not None

    frame = None
    decoder = None
    stubs = False
    for line in proc.stdout:
        # A function header ends with `>:`. The check on the end is faster than the regular expression.
        if line.endswith((">:\n", ">:")) and (m := FUNC.match(line.rstrip())) and not MAPPING_SYMBOL.match(m.group(2)):
            if frame is not None:
                frame.size, frame.cold = decoder.peak, decoder.cold
                yield frame
            frame = None
            if stubs:
                continue
            name = m.group(2)
            decoder = decoder_type(name)
            # Mach-O adds `_` before C names. The demangler removes it from C++ and Rust names.
            if fmt == "macho" and demangle and name.startswith("_"):
                name = name[1:]
            frame = Frame(0, int(m.group(1), 16), name)
            continue
        if (i := line.find("\t")) < 0:
            # A new section ends the function. A section can start without a
            # symbol, e.g. the Mach-O stubs: then its code is not a part of
            # the last function.
            if m := SECTION.match(line.rstrip()):
                if frame is not None:
                    frame.size, frame.cold = decoder.peak, decoder.cold
                    yield frame
                frame = None
                stubs = m.group(1) in STUB_SECTIONS
            continue
        # A line without an address is not an instruction, e.g. `\t\t...`
        # that llvm-objdump prints for skipped zero bytes.
        if frame is None or i == 0 or line[:i].isspace():
            continue
        raw = line[i + 1 :]
        if not raw.strip():
            continue
        decoder.n += 1
        if decoder.dead or decoder.unknown:
            decoder.resume(int(line[:i].strip().rstrip(":"), 16))
        if (kind := decoder.branch_kind(raw)) is not None:
            decoder.branch(kind, raw)
        elif decoder.relevant(raw):
            decoder.step(decoder.normalize(raw))
    if frame is not None:
        frame.size, frame.cold = decoder.peak, decoder.cold
        yield frame

    if proc.wait() != 0:
        sys.exit(f"{objdump} failed with exit code {proc.returncode}")


def merge_cold(frames: list[Frame]):
    """Adds the cold parts of functions to their frames.

    A cold part (`f.cold`) runs on the frame of `f`: its depth starts at
    the depth of the branches from `f` to it. So the frame of `f` is at
    least that depth plus the size of the cold part, and the cold part is
    shown with the same total.
    """
    cold = sorted((f for f in frames if COLD_PART.search(f.name)), key=lambda f: f.address)
    if not cold:
        return
    starts = [f.address for f in cold]
    entry = {}
    for f in frames:
        for address, depth in f.cold.items():
            # The cold part that contains the target address.
            i = bisect.bisect_right(starts, address) - 1
            if i < 0:
                continue
            part = cold[i]
            f.size = max(f.size, depth + part.size)
            entry[id(part)] = max(entry.get(id(part), 0), depth)
    for part in cold:
        part.size += entry.get(id(part), 0)


def main():
    parser = argparse.ArgumentParser(
        description="Print the static stack frame size of each function in an x86_64 or aarch64 binary "
        "(ELF or Mach-O), largest first.",
    )
    parser.add_argument("binary", type=Path, help="path to the binary")
    parser.add_argument(
        "--max",
        type=int,
        metavar="BYTES",
        help="exit with code 1 if a function has a frame larger than BYTES",
    )
    parser.add_argument(
        "--filter",
        type=re.compile,
        metavar="REGEX",
        help="only use the functions whose (demangled) name matches REGEX, e.g. '\\bsql_\\w+::'",
    )
    parser.add_argument(
        "--limit",
        type=int,
        metavar="N",
        help="print only the N largest frames (--max still checks all functions)",
    )
    parser.add_argument("--quiet", "-q", action="store_true", help="do not print the list, only the --max violations")
    parser.add_argument("--no-demangle", action="store_true", help="print mangled symbol names")
    parser.add_argument(
        "--objdump",
        help="objdump executable (default: objdump for ELF files of the host architecture, "
        "else <arch>-linux-gnu-objdump or llvm-objdump)",
    )
    args = parser.parse_args()

    fmt, arch = check_binary(args.binary)
    objdump = args.objdump or default_objdump(fmt, arch)

    all_frames = list(frames(args.binary, fmt, arch, objdump, not args.no_demangle))
    if not all_frames:
        sys.exit(f"{args.binary}: no functions in the disassembly")
    merge_cold(all_frames)
    # shortcut: only the name that objdump shows, not the other aliases at the
    # address; read all names from the symbol table if a check misses a function.
    result = [f for f in all_frames if not args.filter or args.filter.search(f.name)]
    if not result:
        # Else a check with --max and a wrong filter passes and checks nothing.
        sys.exit(f"{args.binary}: no function matches --filter {args.filter.pattern!r}")
    result.sort(key=lambda f: (-f.size, f.name))

    if not args.quiet:
        try:
            for f in result[: args.limit]:
                print(f"{f.size}\t{f.name}")
            sys.stdout.flush()
        except BrokenPipeError:
            # The output goes to e.g. `head`. Still do the --max check.
            sys.stdout = None

    if args.max is not None:
        violations = [f for f in result if f.size > args.max]
        for f in violations:
            print(f"stack frame {f.size} > {args.max} bytes: {f.name} (0x{f.address:x})", file=sys.stderr)
        if violations:
            print(f"{len(violations)} of {len(result)} functions exceed {args.max} bytes", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
