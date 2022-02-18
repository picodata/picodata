# Adding patches

5. Create a temporary git branch
```bash
git checkout -b new-patch
```
4. Add changes to `tarantool-sys` code and commit them with appropriate messages
3. Run format-patch specifying the commit from which the changes branched off
   (currently it's `01023dbc2`)
```bash
git format-patch <hash>
```
2. Reset git branch back to where it started
```bash
git checkout -
```
1. Move the generated `NNNN-<msg>.patch` files into `tarantool-patches`
   directory
