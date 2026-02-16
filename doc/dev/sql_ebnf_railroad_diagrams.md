 # SQL EBNF Railroad Diagrams

This guide describes how to manually generate SVG railroad diagrams for SQL grammar rules.

Related automation ticket: [Issue #2058](https://git.picodata.io/core/picodata/-/issues/2058) tracks automating this workflow.

## Source and destination

- EBNF source: `docs/docs/railroad/query.ebnf`
- Generated diagrams: `docs/docs/images/ebnf/*.svg`

Note: some older notes may refer to `sbroad/doc/query.ebnf`. In this repository, use `docs/docs/railroad/query.ebnf`.

## Generate a diagram in Railroad Diagram Generator

1. Use [RR — Railroad Diagram Generator](https://github.com/GuntherRademacher/rr), which has a public instance at [https://www.bottlecaps.de/rr/ui](https://www.bottlecaps.de/rr/ui).
2. In the top menu, set color scheme: `Options -> #FF4D4D`.
3. Open `docs/docs/railroad/query.ebnf` and copy the needed grammar (or the whole file).
4. Paste grammar into text area on the `Edit` tab.
5. Switch to the `View` tab and confirm the diagram is rendered.
6. Download SVG with these export options:
   - disable `Embedded`
   - enable `XHTML+SVG`
7. Save the file to `docs/docs/images/ebnf/<name>.svg`.

## Post-process the SVG

After download, open the SVG in an editor and apply both fixes:

1. Add a `viewBox` attribute to the `<svg>` tag using the document size. Use the following pattern: `width="x" height="y" viewBox="0 0 x y"`
2. Fix all `xlink:href` links so they are valid for our docs.

## Quick checklist

- File is saved in `docs/docs/images/ebnf/`.
- `<svg>` has a correct `viewBox`.
- `xlink:href` links are corrected.
- Diagram renders correctly in local docs preview.
- We provide a special hook at `docs/hooks/check_svg_attrs.py` that
  validates SVG attributes and embedded links. Any warning is treated as
  error and leads to the `pack-doc` CI job failure.
