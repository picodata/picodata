import { styled } from "@mui/material";
import { CSSProperties } from "react";

export const BoxInfoWrapper = styled("div")({
  padding: "8px 16px",
  display: "flex",
  flexDirection: "column",
});

export const Wrapper = styled("div")({
  display: "flex",
  position: "absolute",
  justifyContent: "center",
  alignItems: "center",
  top: "0px",
  right: "0px",
  bottom: "0px",
  left: "0px",
  backgroundColor: "rgba(107, 114, 128, 0.5)",
  zIndex: 9,
  width: "100%",
  minHeight: "100vh",
});

export const Body = styled("div")({
  backgroundColor: "#fff",
  width: "956px",
  height: "480px",
  borderRadius: "8px",
  padding: "40px 56px",
});

export const TitleWrapper = styled("div")({
  display: "flex",
  justifyContent: "space-between",
  margin: "16px 0",
});

export const TitleTextInline = styled("span")({
  fontSize: "24px",
  fontWeight: 500,
  display: "flex",
  alignItems: "center",
});

export const TitleText = styled("p")({
  display: "flex",
  alignItems: "center",
  margin: 0,
  fontWeight: 400,
  fontSize: "16px",
});

export const CloseElement = styled("div")({
  cursor: "pointer",
  width: "32px",
  height: "32px",
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
});

export const TabGroup = styled("div")({
  display: "flex",
  borderBottom: "1px solid",
  padding: "8px 16px",
});

export const BoxInfoRaw = styled("div")<{ $isGrayRow: boolean }>(
  ({ $isGrayRow }) => ({
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
    padding: "8px 16px",
    ...($isGrayRow
      ? {
          backgroundColor: "#f9f9f8",
        }
      : {}),
  })
);

export const BoxInfo = styled("div")({
  overflow: "auto",
  maxHeight: "380px",
});

export const starIconStyle: CSSProperties = {
  marginRight: "4px",
};
