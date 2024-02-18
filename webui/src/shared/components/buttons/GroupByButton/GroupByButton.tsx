import { ListDashesIcon } from "shared/icons/ListDashesIcon";
import { useTranslation } from "shared/intl";
import {
  ButtonSelect,
  ButtonSelectProps,
} from "shared/ui/ButtonSelect/ButtonSelect";

type GroupByButtonProps<T extends string | number> = Omit<
  ButtonSelectProps<T>,
  "children"
>;

export const GroupByButton = <T extends string | number>(
  props: GroupByButtonProps<T>
) => {
  const { translation } = useTranslation();

  return (
    <ButtonSelect size="small" rightIcon={<ListDashesIcon />} {...props}>
      {translation.components.buttons.groupBy.label}
    </ButtonSelect>
  );
};
