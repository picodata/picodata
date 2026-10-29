// Test cases for tools/binary-stack-usage.py. The name of each function
// starts with its expected frame size: a336_cbz has a 336-byte frame.
	.text
g:	ret
dispatch_spawn:	ret
___chkstk_darwin:	ret
a0_ldp:	mov x8, #0x10
	ldp x0, x8, [x1]
	sub sp, sp, x8
	ret
a0_bic:	mov x8, #0x10
	bic x8, x8, x9
	sub sp, sp, x8
	ret
a0_bfi:	mov x8, #0x10
	bfi x8, x9, #4, #8
	sub sp, sp, x8
	ret
a0_stxr:	mov x8, #0x10
	stxr w8, x1, [x2]
	sub sp, sp, x8
	ret
a0_ldadd:	mov x8, #0x10
	ldadd x1, x8, [x2]
	sub sp, sp, x8
	ret
a16_ldaddsrc:	mov x8, #0x10
	ldadd x8, x1, [x2]
	sub sp, sp, x8
	ret
a16_ldpsrc:	mov x8, #0x10
	ldp x0, x1, [x8]
	sub sp, sp, x8
	ret
a40016_wconst:	stp x29, x30, [sp, #-16]!
	mov w9, #0x9c40
	sub sp, sp, x9
	bl g
	add sp, sp, x9
	ldp x29, x30, [sp], #16
	ret
a131073_movkw:	mov w9, #0x1
	movk w9, #0x2, lsl #16
	sub sp, sp, x9
	ret
a336_cbz:	stp x29, x30, [sp, #-16]!
	sub sp, sp, #0x100
	cbz x0, 1f
	add sp, sp, #0x100
	ldp x29, x30, [sp], #16
	ret
1:	sub sp, sp, #0x40
	bl g
	add sp, sp, #0x140
	ldp x29, x30, [sp], #16
	ret
a336_tbz:	stp x29, x30, [sp, #-16]!
	sub sp, sp, #0x100
	tbz x0, #3, 1f
	add sp, sp, #0x100
	ldp x29, x30, [sp], #16
	ret
1:	sub sp, sp, #0x40
	bl g
	add sp, sp, #0x140
	ldp x29, x30, [sp], #16
	ret
a288_tailcall:	stp x29, x30, [sp, #-16]!
	sub sp, sp, #0x100
	cbz x0, 1f
	add sp, sp, #0x100
	ldp x29, x30, [sp], #16
	b g
1:	sub sp, sp, #0x10
	bl g
	add sp, sp, #0x110
	ldp x29, x30, [sp], #16
	ret
a32_spname:	stp x29, x30, [sp, #-32]!
	bl dispatch_spawn
	ldp x29, x30, [sp], #32
	ret
a576_andsp:	stp x29, x30, [sp, #-16]!
	mov x29, sp
	sub x9, sp, #0x200
	and sp, x9, #0xffffffffffffffc0
	bl g
	mov sp, x29
	ldp x29, x30, [sp], #16
	ret
a16_blclobber:	stp x29, x30, [sp, #-16]!
	mov x8, #0x10
	bl g
	sub sp, sp, x8
	ret
a4112_chkstk:	stp x29, x30, [sp, #-16]!
	mov x15, #0x100
	bl ___chkstk_darwin
	sub sp, sp, x15, uxtx #4
	bl g
	mov sp, x29
	ldp x29, x30, [sp], #16
	ret
a16_wbload:	stp x29, x30, [sp, #-16]!
	mov x8, #0x10
	ldr x0, [x8], #8
	sub sp, sp, x8
	ret
a16_wbstore:	stp x29, x30, [sp, #-16]!
	mov x8, #0x10
	str x0, [x8, #8]!
	sub sp, sp, x8
	ret
a4112_noprobe:	stp x29, x30, [sp, #-16]!
	sub x9, sp, #0x2000
	add x0, x0, #1
	add x1, x1, #1
	add x2, x2, #1
	sub sp, sp, #0x1000
	add sp, sp, #0x1000
	ldp x29, x30, [sp], #16
	ret
a70032_probe:	stp x29, x30, [sp, #-16]!
	sub x9, sp, #0x11, lsl #12
1:	sub sp, sp, #0x1, lsl #12
	ldr xzr, [sp]
	cmp sp, x9
	b.ne 1b
	sub sp, sp, #0x180
	bl g
	add sp, sp, #0x11, lsl #12
	add sp, sp, #0x180
	ldp x29, x30, [sp], #16
	ret
a288_brk:	stp x29, x30, [sp, #-16]!
	cbz x0, 2f
	stp x19, x20, [sp, #-16]!
	sub sp, sp, #0x100
	bl g
	brk #0x1
2:	stp x19, x20, [sp, #-16]!
	sub sp, sp, #0x100
	bl g
	add sp, sp, #0x100
	ldp x19, x20, [sp], #16
	ldp x29, x30, [sp], #16
	ret
a336_padding:	stp x29, x30, [sp, #-16]!
	sub sp, sp, #0x100
	cbnz x0, 1f
	add sp, sp, #0x100
	ldp x29, x30, [sp], #16
	ret
	nop
	nop
1:	sub sp, sp, #0x40
	bl g
	add sp, sp, #0x140
	ldp x29, x30, [sp], #16
	ret
a32_tablecases:	stp x29, x30, [sp, #-16]!
	br x8
1:	sub sp, sp, #0x10
	bl g
	b 3f
2:	sub sp, sp, #0x10
	bl g
	b 3f
4:	sub sp, sp, #0x10
	bl g
	b 3f
3:	add sp, sp, #0x10
	ldp x29, x30, [sp], #16
	ret
// Locals with a large alignment in a frame larger than a page: the probe
// loop moves sp by pages until it passes an aligned register, then sets sp
// to it (clang, C). The last page goes below the register.
a73760_andprobe:	stp x29, x30, [sp, #-32]!
	str x28, [sp, #16]
	mov x29, sp
	sub x9, sp, #0x11, lsl #12
	sub x9, x9, #0x1a0
	and x9, x9, #0xffffffffffffffc0
1:	sub sp, sp, #0x1, lsl #12
	cmp sp, x9
	b.le 2f
	str xzr, [sp]
	b 1b
2:	mov sp, x9
	ldr xzr, [sp]
	mov x0, sp
	bl g
	mov sp, x29
	ldr x28, [sp, #16]
	ldp x29, x30, [sp], #32
	ret
// The same loop rotated, with a second page (rustc).
a73776_andprobeloop:	stp x22, x21, [sp, #-0x30]!
	stp x20, x19, [sp, #0x10]
	stp x29, x30, [sp, #0x20]
	add x29, sp, #0x20
	sub x9, sp, #0x11, lsl #12
	sub x9, x9, #0x250
	and x9, x9, #0xffffffffffffff80
	sub sp, sp, #0x1, lsl #12
	cmp sp, x9
	b.le 2f
1:	str xzr, [sp]
	sub sp, sp, #0x1, lsl #12
	cmp sp, x9
	b.gt 1b
2:	mov sp, x9
	ldr xzr, [sp]
	bl g
	sub sp, x29, #0x20
	ldp x29, x30, [sp, #0x20]
	ldp x20, x19, [sp, #0x10]
	ldp x22, x21, [sp], #0x30
	ret
// A compare of sp with a register that did not end a probe loop.
a4112_cmpfp:	stp x29, x30, [sp, #-16]!
	mov x29, sp
	sub sp, sp, #0x1000
	cmp sp, x29
	b.eq 1f
	bl g
1:	mov sp, x29
	ldp x29, x30, [sp], #16
	ret
// A register that ended a probe loop and then got a new value.
a12304_probereuse:	stp x29, x30, [sp, #-16]!
	sub x9, sp, #0x2000
1:	sub sp, sp, #0x1000
	str xzr, [sp]
	cmp sp, x9
	b.ne 1b
	mov x9, sp
	sub sp, sp, #0x1000
	cmp sp, x9
	b.eq 2f
	str xzr, [sp]
2:	add sp, sp, #0x3000
	ldp x29, x30, [sp], #16
	ret
