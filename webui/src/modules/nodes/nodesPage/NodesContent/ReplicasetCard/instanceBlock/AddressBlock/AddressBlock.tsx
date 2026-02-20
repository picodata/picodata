import { Fragment } from "react/jsx-runtime";

import { useTranslation } from "shared/intl";
import { InfoNoData } from "shared/ui/InfoNoData/InfoNoData";

import { IpAddressLabel } from "../IpAddressLabel/IpAddressLabel";

import { AddressLabel, addressValueSx, Root } from "./StyledComponents";

export interface AddressDisplay {
  title: string;
  value: string;
}

export interface AddressBlockProps {
  addresses: AddressDisplay[];
  className?: string;
}

export const AddressBlock: React.FC<AddressBlockProps> = (props) => {
  const { addresses } = props;
  const { translation } = useTranslation();

  if (addresses.length <= 0)
    return <InfoNoData text={translation.components.infoNoData.label} />;

  return (
    <Root>
      {addresses.map((a) => (
        <Fragment key={a.title}>
          <AddressLabel key={`label-for-${a.title}`}>{a.title}</AddressLabel>
          <IpAddressLabel
            key={`value-for-${a.title}`}
            address={a.value}
            sx={addressValueSx}
          />
        </Fragment>
      ))}
    </Root>
  );
};
