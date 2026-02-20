import { Circle, Root } from "./StyledComponents";

type SwitchInfoProps = {
  checked: boolean;
};

export const SwitchInfo = ({ checked }: SwitchInfoProps) => {
  return (
    <Root>
      <Circle $checked={checked} />
    </Root>
  );
};
