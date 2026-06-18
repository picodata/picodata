import { Box, styled } from "@mui/material";
import { grey } from "@mui/material/colors";
import ErrorIcon from "@mui/icons-material/ErrorOutlineOutlined";

import { useTranslation } from "shared/intl";

const StyleErrorIcon = styled(ErrorIcon)(({ theme }) => ({
  width: 200,
  height: 200,
  color: theme.palette.error.light,
}));
const Root = styled(Box)({
  height: "100%",
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
  flexDirection: "column",
});
const Title = styled(Box)({
  fontWeight: "bold",
  fontSize: 18,
  paddingBottom: 4,
});
const Description = styled(Box)({
  color: grey[600],
  fontSize: 14,
});
export const ErrorMessage = () => {
  const { translation } = useTranslation();
  const errorMessageTranslation =
    translation.pages.instances.list.fullInstanceCard.errorMessage;
  return (
    <Root>
      <StyleErrorIcon />
      <Title>{errorMessageTranslation.title}</Title>
      <Description>{errorMessageTranslation.firstDescription}</Description>
      <Description>{errorMessageTranslation.secondDescription}</Description>
    </Root>
  );
};
