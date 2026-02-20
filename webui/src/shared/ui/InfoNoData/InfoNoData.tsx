import { Root } from "./StyledComponents";

type InfoNoDataProps = {
  text: string;
  className?: string;
};

export const InfoNoData = (props: InfoNoDataProps) => {
  const { text } = props;

  return <Root>{text}</Root>;
};
