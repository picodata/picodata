import React from "react";
import { SxProps } from "@mui/system/styleFunctionSx";

import { InfoNoData } from "shared/ui/InfoNoData/InfoNoData";
import { useTranslation } from "shared/intl";
import { Clippable } from "shared/ui/Clippable/Clippable";

import { containerSx } from "./StyledComponents";

type IpAddressLabelProps = {
  sx?: SxProps;
  address: string;
};

export const IpAddressLabel: React.FC<IpAddressLabelProps> = (props) => {
  const { sx, address } = props;

  const { translation } = useTranslation();

  if (!address)
    return <InfoNoData text={translation.components.infoNoData.label} />;

  return (
    <Clippable
      text={address}
      sx={
        {
          ...containerSx,
          ...sx,
        } as SxProps
      }
    >
      {address}
    </Clippable>
  );
};
