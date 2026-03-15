import {
  AutocompleteProps,
  Box,
  ListItem,
  ListItemText,
  styled,
} from "@mui/material";

import { Expression } from "../../model";

const Flex = styled(Box)({
  display: "flex",
  alignItems: "center",
});

const Left = styled(Flex)(({ theme }) => ({
  color: theme.palette.primary.main,
  justifyContent: "center",
  width: 20,
}));
const Root = styled(Flex)({
  gap: "10px",
});

export const ExpressionOption: AutocompleteProps<
  Expression,
  false,
  false,
  false
>["renderOption"] = (props, { label, description }) => {
  return (
    <ListItem {...props}>
      <ListItemText
        primary={
          <Root>
            <Left>{description}</Left>
            <Flex>{label}</Flex>
          </Root>
        }
      />
    </ListItem>
  );
};
