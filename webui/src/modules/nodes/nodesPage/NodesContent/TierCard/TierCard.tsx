import React, { FC, useState } from "react";
import cn from "classnames";

import { ChevronDown } from "shared/icons/ChevronDown";
import { TierType } from "shared/entity/tier";
import { SwitchInfo } from "shared/ui/SwitchInfo/SwitchInfo";
import { Collapse } from "shared/ui/Collapse/Collapse";
import { InfoNoData } from "shared/ui/InfoNoData/InfoNoData";
import { useTranslation } from "shared/intl";

import { ReplicasetCard } from "../ReplicasetCard/ReplicasetCard";

import styles from "./TierCard.module.scss";

export interface TierCardProps {
  tier: TierType;
}

export const TierCard: FC<TierCardProps> = React.memo(({ tier }) => {
  const [isOpen, setIsOpen] = useState<boolean>(false);

  const { translation } = useTranslation();
  const tierTranslations = translation.pages.instances.list.tierCard;

  const onClick = (event: React.MouseEvent<HTMLDivElement, MouseEvent>) => {
    event.stopPropagation();
    setIsOpen(!isOpen);
  };

  return (
    <div className={styles.cardWrapper} onClick={onClick}>
      <div className={styles.content}>
        <div className={cn(styles.infoColumn, styles.nameColumn)}>
          <div className={styles.label}>{tierTranslations.name.label}</div>
          <div className={styles.infoValue}>{tier.name}</div>
        </div>
        <div className={styles.infoColumn}>
          <div className={styles.label}>{tierTranslations.plugins.label}</div>
          <div className={styles.infoValue}>
            {tier.plugins.length ? (
              tier.plugins.join(", ")
            ) : (
              <InfoNoData text={translation.components.infoNoData.label} />
            )}
          </div>
        </div>
        <div className={styles.infoColumn}>
          <div className={styles.label}>
            {tierTranslations.replicasets.label}
          </div>
          <div className={styles.infoValue}>{tier.replicasetCount}</div>
        </div>
        <div className={styles.infoColumn}>
          <div className={styles.label}>{tierTranslations.instances.label}</div>
          <div className={styles.infoValue}>{tier.instanceCount}</div>
        </div>
        <div className={styles.infoColumn}>
          <div className={styles.label}>{tierTranslations.rf.label}</div>
          <div className={styles.infoValue}>{tier.rf}</div>
        </div>
        <div className={styles.infoColumn}>
          <div className={styles.label}>{tierTranslations.canVote.label}</div>
          <div className={styles.infoValue}>
            <SwitchInfo checked={tier.can_vote} />
          </div>
        </div>
        <div className={cn(styles.infoColumn, styles.chevronColumn)}>
          <ChevronDown
            className={cn(styles.chevronIcon, isOpen && styles.chevronIconOpen)}
          />
        </div>
      </div>
      <Collapse isOpen={isOpen}>
        <div className={styles.replicasetsWrapper}>
          {tier.replicasets.map((replicaset) => (
            <ReplicasetCard
              key={replicaset.id}
              replicaset={replicaset}
              theme="secondary"
            />
          ))}
        </div>
      </Collapse>
    </div>
  );
});
