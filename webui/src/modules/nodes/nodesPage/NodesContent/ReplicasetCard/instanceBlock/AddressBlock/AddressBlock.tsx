import cn from "classnames";
import { Fragment } from "react/jsx-runtime";

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
        <Fragment key={a.title}>
          <span className={styles.addressLabel} key={`label-for-${a.title}`}>
            {a.title}
          </span>
          <IpAddressLabel
            key={`value-for-${a.title}`}
            className={styles.addressValue}
            address={a.value}
          />
        </Fragment>
      ))}
    </div>
  );
};
