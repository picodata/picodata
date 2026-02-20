import { keyframes, styled } from "@mui/material";

export const spin = keyframes`
  from { transform: rotate(0deg); }
  to { transform: rotate(360deg); }
`;

export const Root = styled("svg")({
  transformOrigin: "center",
  animation: `${spin} 1s linear infinite`,
});
