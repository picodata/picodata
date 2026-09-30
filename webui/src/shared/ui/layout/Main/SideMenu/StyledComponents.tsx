import { styled } from "@mui/material";
import { NavLink } from "react-router-dom";

export const Root = styled("div")<{ $isOpen: boolean }>(
  ({ theme, $isOpen }) => ({
    width: $isOpen ? "288px" : "64px",
    flexShrink: 0,
    overflow: "hidden",
    overflowY: $isOpen ? "auto" : "hidden",
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
    left: 0,
    top: 48,
    bottom: 0,
    padding: "40px 16px",
    zIndex: theme.common.zIndex.zIndexSideMenu,
    transition: " width 0.3s ease-out",
    position: "absolute",
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
    display: $isOpen ? "block" : "none",
  })
);

export const NavLinkText = styled("span")({
  fontSize: "16px",
  fontWeight: 500,
  lineHeight: "30px",
  userSelect: "none",
});

export const MenuNavLink = styled(NavLink)(({ theme }) => ({
  display: "flex",
  alignItems: "center",
  columnGap: "16px",
  color: theme.common.colors.typography.colorTextBlack,
  marginBottom: "16px",
  "&:last-child": { marginBottom: 0 },
  "&:hover, &[aria-current='page']": {
    color: theme.common.colors.typography.colorTextOrange,
  },
}));

export const PluginGroup = styled("details")({
  marginTop: "16px",
  overflowWrap: "anywhere",
});

export const PluginSummary = styled("summary")({
  display: "flex",
  alignItems: "center",
  columnGap: "16px",
  cursor: "pointer",
  fontSize: "16px",
  fontWeight: 500,
  listStyle: "none",
  "&::-webkit-details-marker": { display: "none" },
  "&::before": {
    content: "'▶'",
    width: "24px",
    flexShrink: 0,
    fontSize: "12px",
    textAlign: "center",
  },
  "details[open] > &::before": {
    transform: "rotate(90deg)",
  },
});

export const PluginPages = styled("div")({
  paddingTop: "12px",
});

export const PluginPageIconSlot = styled("span")({
  display: "inline-flex",
  alignItems: "center",
  justifyContent: "center",
  flexShrink: 0,
  width: "24px",
  height: "24px",
});

export const PluginPageIcon = styled("img")({
  width: "20px",
  height: "20px",
  objectFit: "contain",
});
