import { FunnelIcon } from "shared/icons/FunnelIcon";
import { useTranslation } from "shared/intl";
import { Button, ButtonProps } from "shared/ui/Button/Button";

type FilterByButtonProps = Omit<ButtonProps, "children">;

export const FilterByButton = (props: FilterByButtonProps) => {
  const { translation } = useTranslation();

  return (
    <Button size="small" rightIcon={<FunnelIcon />} {...props}>
      {translation.components.buttons.filterBy.label}
    </Button>
  );
};
