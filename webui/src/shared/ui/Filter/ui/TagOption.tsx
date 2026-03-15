import {
  AutocompleteProps,
  Box,
  ListItem,
  ListItemText,
  styled,
} from "@mui/material";

import { Tag } from "../model";

const Flex = styled(Box)({
  display: "flex",
  alignItems: "center",
});
const Root = styled(Flex)({
  gap: "10px",
});

export const TagOption: AutocompleteProps<
  Tag,
  false,
  false,
  false
>["renderOption"] = (props, { label, icon: Icon }) => {
  return (
    <ListItem {...props}>
      <ListItemText
        primary={
          <Root>
            {Icon ? (
              <Flex>
                <Icon fontSize={"small"} color={"primary"} />
              </Flex>
            ) : null}
            <Flex>{label}</Flex>
          </Root>
        }
      />
    </ListItem>
  );
};
