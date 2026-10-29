# Test cases for tools/binary-stack-usage.py. The name of each function
# starts with its expected frame size: e328_earlyret has a 328-byte frame.
	.text
g:	ret
compute_size:	mov $1,%eax
	ret
___chkstk_darwin:	ret
e136_negimm:	push %rbp
	add $-128,%rsp
	call g
	sub $-128,%rsp
	pop %rbp
	ret
e8_mulrax:	push %rbx
	mov $5,%eax
	mul %rcx
	sub %rax,%rsp
	ret
e8_callrax:	push %rbx
	mov $5,%eax
	call compute_size
	sub %rax,%rsp
	ret
e70024_chkstk:	push %rbp
	mov $0x11180,%eax
	call ___chkstk_darwin
	sub %rax,%rsp
	add %rax,%rsp
	pop %rbp
	ret
e328_earlyret:	push %rbx
	sub $0x100,%rsp
	test %edi,%edi
	je 1f
	add $0x100,%rsp
	pop %rbx
	ret
1:	sub $0x40,%rsp
	call g
	add $0x140,%rsp
	pop %rbx
	ret
e72_tailcall:	push %rbx
	sub $0x30,%rsp
	test %edi,%edi
	je 1f
	add $0x30,%rsp
	pop %rbx
	jmp g
1:	sub $0x10,%rsp
	call g
	add $0x40,%rsp
	pop %rbx
	ret
e24_loopjmp:	push %rbx
	jmp 2f
1:	sub $0x10,%rsp
	call g
	add $0x10,%rsp
2:	dec %edi
	jne 1b
	pop %rbx
	ret
e56_jumptable:	push %rbx
	sub $0x28,%rsp
	jmp *(%rdi)
1:	call g
	add $0x28,%rsp
	pop %rbx
	ret
2:	sub $0x8,%rsp
	call g
	add $0x30,%rsp
	pop %rbx
	ret
e8_r11scratch:	push %rbx
	mov 0x8(%rdi),%r11
	sub $0x8,%r11
	mov %rdi,%rax
1:	sub $0x1000,%rsp
	orb $0,(%rsp)
	cmp %rax,%rsp
	jne 1b
	pop %rbx
	ret
e70016_r11probe:	mov %rsp,%r11
	sub $0x11000,%r11
1:	sub $0x1000,%rsp
	movq $0,(%rsp)
	cmp %r11,%rsp
	jne 1b
	sub $0x180,%rsp
	call g
	add $0x11180,%rsp
	ret
e70016_leaprobe:	lea -0x11000(%rsp),%r11
1:	sub $0x1000,%rsp
	orb $0,0xff8(%rsp)
	cmp %r11,%rsp
	jne 1b
	sub $0x180,%rsp
	call g
	add $0x11180,%rsp
	ret
e64_lea:	lea -0x40(%rsp),%rsp
	call g
	lea 0x40(%rsp),%rsp
	ret
e16_repzret:	push %rbx
	push %rbp
	test %edi,%edi
	je 1f
	pop %rbp
	pop %rbx
	repz ret
1:	pop %rbp
	pop %rbx
	ret
e264_ud2:	test %edi,%edi
	je 2f
	push %rbx
	sub $0x100,%rsp
	test %esi,%esi
	jne 3f
	call g
	ud2
2:	push %rbx
	sub $0x100,%rsp
	call g
3:	add $0x100,%rsp
	pop %rbx
	ret
e264_leave:	push %rbp
	mov %rsp,%rbp
	sub $0x100,%rsp
	call g
	leave
	ret
	sub $0x10,%rsp
	call g
	add $0x10,%rsp
	ret
e72_learbp:	push %rbp
	mov %rsp,%rbp
	push %rbx
	sub $0x38,%rsp
	call g
	lea -0x8(%rbp),%rsp
	pop %rbx
	pop %rbp
	ret
	sub $0x40,%rsp
	call g
	add $0x40,%rsp
	ret
e10_pushfw:	pushfq
	pushw $1
	popw %ax
	popfq
	ret
e272_learsp:	push %rbp
	push %rbx
	lea 0x8(%rsp),%rbp
	sub $0x100,%rsp
	call g
	mov %rbp,%rsp
	sub $0x8,%rsp
	call g
	add $0x8,%rsp
	pop %rbp
	ret
e120_andrsp:	push %rbp
	mov %rsp,%rbp
	and $-64,%rsp
	sub $0x40,%rsp
	call g
	mov %rbp,%rsp
	pop %rbp
	ret
	sub $0x10,%rsp
	ret
e328_cold:	push %rbx
	sub $0x100,%rsp
	test %edi,%edi
	jne e328_cold.cold
	add $0x100,%rsp
	pop %rbx
	ret
e328_cold.cold:	sub $0x40,%rsp
	call g
	ud2
# A jump inside a cold part is not a jump to a cold part.
e1096_coldjump:	push %rbx
	sub $0x100,%rsp
	test %edi,%edi
	jne e1096_coldjump.cold
	add $0x100,%rsp
	pop %rbx
	ret
e1096_coldjump.cold:	sub $0x40,%rsp
	test %esi,%esi
	jne 1f
	add $0x40,%rsp
	call g
	ud2
1:	sub $0x300,%rsp
	call g
	ud2
e328_padding:	push %rbx
	sub $0x100,%rsp
	test %edi,%edi
	jne 1f
	add $0x100,%rsp
	pop %rbx
	ret
	nopw 0x0(%rax,%rax,1)
	nop
1:	sub $0x40,%rsp
	call g
	add $0x140,%rsp
	pop %rbx
	ret
e64_tablecases:	push %rbx
	sub $0x30,%rsp
	jmp *(%rdi)
1:	sub $0x8,%rsp
	call g
	jmp 3f
2:	sub $0x8,%rsp
	call g
	jmp 3f
4:	sub $0x8,%rsp
	call g
	jmp 3f
3:	add $0x38,%rsp
	pop %rbx
	ret
# A C++ function with a cold part: objdump shows the demangled names
# `e33032_plus::operator+(e33032_plus)` and the same with ` (.cold)` or
# ` [clone .cold]`.
_ZN11e33032_plusplES_:	push %rbx
	sub $0x100,%rsp
	test %edi,%edi
	jne _ZN11e33032_plusplES_.cold
	add $0x100,%rsp
	pop %rbx
	ret
_ZN11e33032_plusplES_.cold:	sub $0x8000,%rsp
	call g
	ud2
# Locals with an alignment of 64 KiB: the probe loop goes down to an
# aligned %r11, then a probe for the frame follows (clang).
e131064_andr11:	push %rbp
	mov %rsp,%rbp
	mov %rsp,%r11
	and $-0x10000,%r11
	cmp %rsp,%r11
	je 3f
	sub $0x1000,%rsp
	cmp %r11,%rsp
	jb 2f
1:	movq $0,(%rsp)
	sub $0x1000,%rsp
	cmp %rsp,%r11
	jb 1b
2:	mov %r11,%rsp
	movq $0,(%rsp)
3:	mov %rsp,%r11
	sub $0x10000,%r11
4:	sub $0x1000,%rsp
	movq $0,(%rsp)
	cmp %r11,%rsp
	jne 4b
	mov %rsp,%rdi
	call g
	mov %rbp,%rsp
	pop %rbp
	ret
