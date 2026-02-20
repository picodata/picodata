import {
  GlobalStyles as MuiGlobalStyles,
  GlobalStylesProps,
} from "@mui/material";

const globalStyles: GlobalStylesProps["styles"] = () => ({
  "#root": {
    fontFamily: "Montserrat, sans-serif",
    lineHeight: 1.5,
    fontWeight: 400,
  },
  html: {
    width: "100vw",
    height: "100vh",
    overflow: "auto",
    boxSizing: "border-box",
    "& *:before, & *:after": {
      boxSizing: "inherit",
    },
  },
  body: {
    width: "100vw",
    height: "100vh",
    overflow: "auto",
  },
  a: {
    fontWeight: 500,
    color: "#646cff",
    textDecoration: "inherit",
    "&:hover": {
      color: "#535bf2",
    },
  },
  h1: {
    fontSize: "3.2em",
    lineHeight: 1.1,
  },
  button: {
    fontFamily: "inherit",
  },
});

export const GlobalStyles = () => {
  return <MuiGlobalStyles styles={globalStyles} />;
};
