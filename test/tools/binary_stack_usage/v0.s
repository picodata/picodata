# Functions with Rust v0 mangled names: `sql_ir::e8_push`, `sql_ir::e24_sub`
# and `other::e16_sub`. A filter on the crate needs them demangled.
	.text
_RNvCs1a2b3c_6sql_ir7e8_push:	push %rbx
	pop %rbx
	ret
_RNvCs1a2b3c_6sql_ir7e24_sub:	sub $24,%rsp
	add $24,%rsp
	ret
_RNvCs1a2b3c_5other7e16_sub:	sub $16,%rsp
	add $16,%rsp
	ret
