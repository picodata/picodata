import { styled } from "@mui/material";

export const Root = styled("div")<{ $isOpen: boolean }>(
  ({ theme, $isOpen }) => ({
    width: $isOpen ? "192px" : "64px",
    flexShrink: 0,
    overflow: "hidden",
    background: $isOpen
      ? `linear-gradient(
    176.39deg,
    rgba(231, 222, 222, 0.28) 0%,
    rgba(230, 206, 192, 0.32) 36.98%,
    rgba(217, 208, 206, 0.16) 69.79%,
    rgba(231, 225, 223, 0.28) 100%
  )`
      : `linear-gradient(
    176.39deg,
    rgba(231, 222, 222, 0.3) 0%,
    rgba(243, 238, 233, 0.3) 36.98%,
    rgba(225, 215, 213, 0.24) 69.79%,
    rgba(231, 225, 223, 0.3) 100%
  )`,
    backdropFilter: "blur(10px)", // возможны тормоза в хроме из блюров, работать с ними аккуратнее
    position: "absolute",
    left: 0,
    top: 0,
    bottom: 0,
    padding: "40px 16px",
    zIndex: theme.common.zIndex.zIndexSideMenu,
    transition: " width 0.3s ease-out",
  })
);

export const MenuIcon = styled("div")<{ $isOpen: boolean }>(
  ({ theme, $isOpen }) => ({
    display: "inline-flex",
    alignItems: "center",
    justifyContent: "center",
    color: $isOpen
      ? theme.common.colors.typography.colorTextBlack
      : theme.common.colors.typography.colorTextGrey,
    cursor: "pointer",
  })
);

export const NavLinksList = styled("div")<{ $isOpen: boolean }>(
  ({ $isOpen }) => ({
    marginTop: "80px",
    opacity: $isOpen ? 1 : 0,
  })
);

export const NavLinkText = styled("span")({
  fontSize: "16px",
  fontWeight: 500,
  lineHeight: "30px",
  userSelect: "none",
});
