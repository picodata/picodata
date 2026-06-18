import { ModalProps as MuiModalProps, SxProps, Theme } from "@mui/material";
import { PropsWithChildren, ReactNode } from "react";

export type ModalProps = PropsWithChildren<{
  muiModalProps?: Partial<MuiModalProps>;
  header?: ReactNode;
  footer?: ReactNode;
  title: ReactNode;
  open: boolean;
  onClose: () => void;
  sx?: SxProps<Theme>;
}>;

export type ModalHeaderHeaderProps = ModalToolBarProps & {
  onCloseClick?: () => void;
  onFullScreenClick?: () => void;
  fullScreen?: boolean;
};
export type ModalFooterProps = ModalToolBarProps & {
  onOkClickHandler?: () => void;
};

export type ModalToolBarProps = {
  leftContent?: ReactNode;
  centerContent?: ReactNode;
  rightContent?: ReactNode;
};
