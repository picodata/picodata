import {
  Box,
  BoxProps,
  Theme,
  Typography,
  styled,
  IconButton,
} from "@mui/material";
import {
  SnackbarProvider as NotistackSnackbarProvider,
  SnackbarKey,
  SnackbarProviderProps,
  VariantType,
  useSnackbar as useNotistackSnackbar,
} from "notistack";
import {
  forwardRef,
  PropsWithChildren,
  ReactNode,
  useCallback,
  useState,
} from "react";
import CheckCircleOutlineOutlinedIcon from "@mui/icons-material/CheckCircleOutlineOutlined";
import ErrorOutlineOutlinedIcon from "@mui/icons-material/ErrorOutlineOutlined";
import WarningAmberOutlinedIcon from "@mui/icons-material/WarningAmberOutlined";
import InfoOutlinedIcon from "@mui/icons-material/InfoOutlined";
import CloseOutlinedIcon from "@mui/icons-material/CloseOutlined";
import ArrowCircleUpOutlinedIcon from "@mui/icons-material/ArrowCircleUpOutlined";
import ArrowCircleDownOutlinedIcon from "@mui/icons-material/ArrowDownwardOutlined";

import { useTranslation } from "../../intl";
type SnackBarIconProps = {
  type: VariantType;
};

const colorByType = (statusType: string, theme: Theme) => {
  switch (true) {
    case statusType === "error":
      return theme.palette.error.main;
    case statusType === "success":
      return theme.palette.success.main;
    case statusType === "warning":
      return theme.palette.warning.main;
    default:
      return theme.palette.info.main;
  }
};

const SnackBarIcon = ({ type }: SnackBarIconProps) => {
  switch (type) {
    case "success":
      return <CheckCircleOutlineOutlinedIcon color={"success"} />;
    case "error":
      return <ErrorOutlineOutlinedIcon color={"error"} />;
    case "warning":
      return <WarningAmberOutlinedIcon color={"warning"} />;
    case "info":
      return <InfoOutlinedIcon color={"info"} />;
    default:
      return <InfoOutlinedIcon color={"info"} />;
  }
};

const Root = styled(Box)(({ theme }) => ({
  background: theme.palette.background.default,
  boxShadow: "2px 2px 4px 2px rgba(34, 60, 80, 0.2)",
  overflow: "hidden",
  width: 324,
  borderRadius: 8,
  display: "grid",
  gridTemplateAreas: `
        'line icon title close'
        'line icon description close'
        'line icon details-button close'
        'line icon-details details close-details'
    `,
  gridTemplateColumns: "5px min-content 1fr min-content",
  gridTemplateRows: "min-content min-content min-content min-content",
}));

const GreyBox = styled(Box)(({ theme }) => ({
  background: theme.palette.background.default,
}));

const LineBox = styled(Box, {
  shouldForwardProp: (prop) => prop !== "statusType",
})<BoxProps & { statusType: string }>(({ statusType, theme }) => ({
  background: colorByType(statusType, theme),
}));

const IconBox = styled(Box)({
  padding: 12,
});

const TitleBox = styled(Box)({
  paddingTop: 12,
  fontWeight: 600,
  fontSize: 14,
});

const DescriptionBox = styled(Box)({
  fontSize: 12,
  paddingBottom: 12,
  paddingTop: 12,
});

const DetailsBox = styled(GreyBox)({
  paddingBottom: 12,
  fontSize: 11,
  wordWrap: "break-word",
  minWidth: "100%",
  overflowY: "auto",
  maxHeight: "33vh",
});

const DetailButtonRoot = styled(Box)({
  display: "flex",
  gridArea: "details-button",
  alignItems: "center",
  paddingTop: 12,
  paddingBottom: 12,
  cursor: "pointer",
});

const DetailButtonText = styled(Typography)({
  fontWeight: 600,
  fontSize: 11,
});

type DetailButtonProps = {
  open: boolean;
  onClick: () => void;
};
const DetailButton = ({ open, onClick }: DetailButtonProps) => {
  const { translation } = useTranslation();
  return (
    <DetailButtonRoot onClick={onClick}>
      <DetailButtonText>{translation.common.moreDetailed}</DetailButtonText>
      {!open ? <ArrowCircleDownOutlinedIcon /> : <ArrowCircleUpOutlinedIcon />}
    </DetailButtonRoot>
  );
};
export type Message = {
  title?: ReactNode;
  description?: ReactNode;
  detail?: ReactNode;
  type: VariantType;
};

type SnackbarMessage = {
  message: Message;
  id: SnackbarKey;
};
export const Snackbars = forwardRef<HTMLDivElement, SnackbarMessage>(
  ({ id, message }, ref) => {
    const { closeSnackbar } = useNotistackSnackbar();
    const [detailOpen, setDeatailOpen] = useState(false);

    const detailButtonClickHandler = () => {
      setDeatailOpen((_detailOpen) => !_detailOpen);
    };

    const closeHandler = () => {
      closeSnackbar(id);
    };

    return (
      <Root ref={ref}>
        <LineBox gridArea={"line"} statusType={message.type} />
        <IconBox gridArea={"icon"}>
          <SnackBarIcon type={message.type} />
        </IconBox>
        <Box gridArea={"close"}>
          <IconButton onClick={closeHandler}>
            <CloseOutlinedIcon />
          </IconButton>
        </Box>
        {message.title ? (
          <TitleBox gridArea={"title"}>{message.title}</TitleBox>
        ) : null}
        {message.description ? (
          <DescriptionBox gridArea={"description"}>
            {message.description}
          </DescriptionBox>
        ) : null}
        {message.detail ? (
          <>
            <DetailButton
              open={detailOpen}
              onClick={detailButtonClickHandler}
            />
            {detailOpen ? (
              <>
                <DetailsBox gridArea={"details"}>{message.detail}</DetailsBox>
                <GreyBox gridArea={"icon-details"} />
                <GreyBox gridArea={"close-details"} />
              </>
            ) : null}
          </>
        ) : null}
      </Root>
    );
  }
);

export const useShowSnackBar = () => {
  const { enqueueSnackbar } = useNotistackSnackbar();

  return useCallback(
    (message: Message) => {
      enqueueSnackbar("", {
        variant: message.type,
        autoHideDuration: 10000,
        content: (id) => <Snackbars id={id} message={message} />,
      });
    },
    [enqueueSnackbar]
  );
};

export const SnackBarProvider = (
  props: PropsWithChildren<SnackbarProviderProps>
) => (
  <NotistackSnackbarProvider
    maxSnack={4}
    anchorOrigin={{
      vertical: "bottom",
      horizontal: "right",
    }}
    {...props}
  />
);
