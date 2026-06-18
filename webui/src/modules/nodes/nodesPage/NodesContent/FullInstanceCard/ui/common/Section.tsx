import { Box, styled, SxProps, Tooltip, Link } from "@mui/material";
import { PropsWithChildren, ReactNode } from "react";
import { grey } from "@mui/material/colors";

export const SectionFieldsContent = styled(Box)({
  display: "grid",
  gridTemplateColumns: "180px auto",
});
const Centered = styled(Box)({
  display: "flex",
  alignItems: "center",
  paddingTop: 4,
  paddingBottom: 4,
});
export const StyledLink = styled(Link)<{ $hasLink?: boolean }>(
  ({ $hasLink, theme }) => ({
    color: grey[500],
    textDecoration: "none",
    cursor: "default",
    "&: hover": {
      color: grey[500],
    },
    ...($hasLink
      ? {
          cursor: "pointer",
          "&: hover": {
            cursor: "pointer",
            color: theme.palette.primary.main,
          },
        }
      : {}),
  })
);
const SectionFieldLabel = styled(StyledLink)<{ $hasLink: boolean }>({
  fontSize: 14,
  display: "flex",
  alignItems: "center",
  paddingTop: 4,
  paddingBottom: 4,
});
const SectionFieldValue = styled(Centered)({
  overflow: "hidden",
  fontSize: 14,
});
export const EllipsisBlock = styled(Box)({
  width: "100%",
  overflow: "hidden",
  whiteSpace: "nowrap",
  textOverflow: "ellipsis",
});

const Root = styled(Box)({
  display: "flex",
  flexDirection: "column",
  gap: 10,
});
const Title = styled(Box)({
  fontSize: 14,
  letterSpacing: 0.8,
  fontWeight: 500,
});

type SectionProps = PropsWithChildren<{
  title: string;
}>;
export const Section = ({ title, children }: SectionProps) => {
  return (
    <Root>
      <Title>{title}</Title>
      <Box>{children}</Box>
    </Root>
  );
};

export type SectionFieldProps = PropsWithChildren<{
  name: ReactNode;
  title?: ReactNode;
  withTitle?: boolean;
  labelSx?: SxProps;
  link?: string;
}>;
export const SectionField = ({
  name,
  children,
  title,
  withTitle = true,
  labelSx,
  link,
}: SectionFieldProps) => {
  return (
    <>
      <SectionFieldLabel
        sx={labelSx}
        href={link}
        $hasLink={Boolean(link)}
        target={"_blank"}
      >
        {name}
      </SectionFieldLabel>
      <SectionFieldValue>
        <Tooltip title={withTitle ? title ?? children : undefined}>
          <EllipsisBlock>{children ?? "-"}</EllipsisBlock>
        </Tooltip>
      </SectionFieldValue>
    </>
  );
};
