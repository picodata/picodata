import cn from "classnames";

import { useTranslation } from "shared/intl";
import { InfoNoData } from "shared/ui/InfoNoData/InfoNoData";

import { IpAddressLabel } from "../IpAddressLabel/IpAddressLabel";

import styles from "./AddressBlock.module.scss";

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
    <div className={cn(styles.addressBlock, props.className)}>
      {addresses.map((a) => (
        <>
          <span className={styles.addressLabel} key={a.value}>
            {a.title}
          </span>
          <IpAddressLabel
            key={a.value}
            className={styles.addressValue}
            address={a.value}
          />
        </>
      ))}
    </div>
  );
};
