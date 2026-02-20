import { Root } from "./StyledComponents";

type OptionProps = {
  isSelected: boolean;
  children: React.ReactNode;
} & React.ButtonHTMLAttributes<HTMLDivElement>;

export const Option = (props: OptionProps) => {
  const { isSelected, children, ...other } = props;

  return (
    <Root {...other} $isSelected={isSelected}>
      {children}
    </Root>
  );
};
