import React from "react";

import { CloseIcon, Root, TitleText, TitleWrapper } from "./StyledComponents";

export type ModalBodyProps = {
  title: React.ReactNode;
  children: (args: {
    onClose: () => void;
  }) => Exclude<React.ReactNode, "string"> | React.ReactNode;
  onClose: () => void;
} & Omit<React.HtmlHTMLAttributes<HTMLDivElement>, "children">;

export const ModalBody: React.FC<ModalBodyProps> = (props) => {
  const { title, children, onClose, ...other } = props;

  return (
    <Root {...other}>
      <TitleWrapper>
        <TitleText /*className={styles.titleText}*/>{title}</TitleText>
        <CloseIcon onClick={onClose} />
      </TitleWrapper>
      {typeof children === "function" ? children({ onClose }) : children}
    </Root>
  );
};
