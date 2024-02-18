import { ArrowsUpDown } from "shared/icons/ArrowsUpDown";
import { useTranslation } from "shared/intl";
import {
  ButtonSelect,
  ButtonSelectProps,
} from "shared/ui/ButtonSelect/ButtonSelect";

type SortByButtonProps<T extends string | number> = Omit<
  ButtonSelectProps<T>,
  "children"
> & {
  onIconClick: () => void;
};

export const SortByButton = <T extends string | number>(
  props: SortByButtonProps<T>
) => {
  const { onIconClick, ...other } = props;
  const { translation } = useTranslation();

  return (
    <ButtonSelect
      size="small"
      rightIcon={
        <ArrowsUpDown
          onClick={(event) => {
            event.stopPropagation();
            onIconClick();
          }}
        />
      }
      {...other}
    >
      {translation.components.buttons.sortBy.label}
    </ButtonSelect>
  );
};
