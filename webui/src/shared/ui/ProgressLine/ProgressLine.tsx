import { Fill, Root } from "./StyledComponents";

export type ProgressLineProps = {
  width: number | string;
  height: number | string;
  percent: number;
  strokeColor?: string;
  trailColor?: string;
};

export const ProgressLine = (props: ProgressLineProps) => {
  const { trailColor = "#F9F5F2", strokeColor = "#6FFF9F" } = props;

  return (
    <Root
      style={{
        width: props.width,
        height: props.height,
        backgroundColor: trailColor,
      }}
    >
      <Fill
        style={{
          width: `${props.percent}%`,
          backgroundColor: strokeColor,
        }}
      />
    </Root>
  );
};
