import { Box, styled } from "@mui/material";
import ContentCopyIcon from "@mui/icons-material/ContentCopy";

import { HiddenBox } from "shared/ui/HiddenBox";
import { useShowSnackBar } from "shared/ui/SnackBar/SnackBar";
import { useTranslation } from "shared/intl";

import { EllipsisBlock } from "../common";

const StyledContentCopyIcon = styled(ContentCopyIcon)(({ theme }) => ({
  width: 14,
  height: 14,
  color: theme.palette.primary.main,
}));

const Root = styled(Box)({
  display: "flex",
  gap: 6,
  alignItems: "center",
  cursor: "pointer",
});

type AddressProps = {
  value?: string | null;
};
export const Address = ({ value }: AddressProps) => {
  const showSnackBar = useShowSnackBar();
  const { translation } = useTranslation();
  const copyTextTranslation = translation.common.messages.copyText;
  const clickHandler = () => {
    try {
      navigator.clipboard.writeText(value || "");
      showSnackBar({
        title: copyTextTranslation.title,
        description: copyTextTranslation.successDescription,
        type: "success",
      });
    } catch (e) {
      showSnackBar({
        title: copyTextTranslation.title,
        description: copyTextTranslation.errorDescription,
        type: "error",
      });
    }
  };

  return (
    <Root onClick={clickHandler}>
      {value ? (
        <>
          <HiddenBox>
            <EllipsisBlock>{value}</EllipsisBlock>
          </HiddenBox>
          <StyledContentCopyIcon />
        </>
      ) : (
        "-"
      )}
    </Root>
  );
};
