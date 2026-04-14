import SearchIcon from "@mui/icons-material/Search";
import { Children, cloneElement, ReactElement, ReactNode } from "react";
import { Box, styled } from "@mui/material";

const Root = styled(Box)({
  height: "100%",
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
  gap: "1px",
});
const SearchIconContainer = styled(Box)(({ theme }) => ({
  borderLeft: `1px solid ${theme.palette.grey["500"]}`,
  paddingLeft: "4px",
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
}));
const StyledSearchIcon = styled(SearchIcon)(({ theme }) => ({
  color: theme.palette.grey["500"],
}));
export const setInputEndAdornment = (node: ReactElement): ReactNode => {
  const children = Children.toArray(node.props.children);

  children.push(
    <SearchIconContainer>
      <StyledSearchIcon />
    </SearchIconContainer>
  );
  const result = <Root>{children}</Root>;
  return cloneElement(node, {}, result);
};
