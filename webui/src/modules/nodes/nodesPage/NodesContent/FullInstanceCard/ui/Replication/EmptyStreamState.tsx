import LinkOffOutlinedIcon from "@mui/icons-material/LinkOffOutlined";
import { Box, styled } from "@mui/material";
import { grey } from "@mui/material/colors";

import { useTranslation } from "shared/intl";

import { Label } from "./Common";

const Root = styled(Box)({
  height: "100%",
  padding: 10,
  display: "flex",
  flexDirection: "column",
});

const Content = styled(Box)({
  flex: 1,
  display: "flex",
  flexDirection: "column",
  alignItems: "center",
  justifyContent: "center",
  gap: 6,
  color: grey[500],
  textAlign: "center",
});

const Icon = styled(LinkOffOutlinedIcon)({
  width: 32,
  height: 32,
});

const Title = styled(Box)({
  color: grey[800],
  fontSize: 13,
  fontWeight: 600,
});

const Description = styled(Box)({
  maxWidth: 260,
  fontSize: 12,
});

type EmptyStreamStateProps = {
  streamType: "upstream" | "downstream";
};

export const EmptyStreamState = ({ streamType }: EmptyStreamStateProps) => {
  const { translation } = useTranslation();
  const replicationContentTranslation =
    translation.pages.instances.list.fullInstanceCard.replicationContent;
  const commonTranslation = translation.common;

  return (
    <Root>
      <Label>{streamType}</Label>
      <Content>
        <Icon />
        <Title>{commonTranslation.noData}</Title>
        <Description>
          {replicationContentTranslation.connectionInfoUnavailable}
        </Description>
      </Content>
    </Root>
  );
};
