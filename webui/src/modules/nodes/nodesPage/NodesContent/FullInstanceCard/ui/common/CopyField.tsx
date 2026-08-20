import { Box, styled } from "@mui/material";
import ContentCopyIcon from "@mui/icons-material/ContentCopy";
import { isValidElement, PropsWithChildren } from "react";

import { HiddenBox } from "shared/ui/HiddenBox";
import { useShowSnackBar } from "shared/ui/SnackBar/SnackBar";
import { useTranslation } from "shared/intl";
import { copyText } from "shared/utils/copyText";

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

type CopyFieldProps = {
  text?: string | null;
};
export const CopyField = ({
  text,
  children,
}: PropsWithChildren<CopyFieldProps>) => {
  const showSnackBar = useShowSnackBar();
  const { translation } = useTranslation();
  const copyTextTranslation = translation.common.messages.copyText;
  const clickHandler = async () => {
    const copied = await copyText(text || "");
    showSnackBar({
      title: copyTextTranslation.title,
      description: copied
        ? copyTextTranslation.successDescription
        : copyTextTranslation.errorDescription,
      type: copied ? "success" : "error",
    });
  };

  const hasContent =
    (typeof children === "string" && children) ||
    (isValidElement(children) &&
      typeof children.props.children === "string" &&
      children.props.children);

  return hasContent ? (
    <Root onClick={text ? clickHandler : undefined}>
      <HiddenBox>
        <EllipsisBlock>{children}</EllipsisBlock>
      </HiddenBox>
      <StyledContentCopyIcon />
    </Root>
  ) : (
    <>-</>
  );
};
