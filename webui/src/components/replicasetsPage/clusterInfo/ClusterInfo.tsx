import { CapacityIcon } from "components/icons/CapacityIcon";
import styles from "./ClusterInfo.module.css";

export const ClusterInfo = () => (
  <div className={styles.wrapper}>
    <div className={styles.infoColumn}>
      <p className={styles.columnName}>Capacity Usage</p>
      <div className={styles.capacityWrapper}>
        <div className={styles.flexWrapper}>
          <p className={styles.boldText}>30%</p>
          <CapacityIcon />
        </div>
      </div>

      <div className={styles.textWrapper}>
        <div className={styles.textBlock}>
          <p className={styles.greyText}>Used</p>
          <p className={styles.boldText}> 3 GB</p>
        </div>
        <div className={styles.textBlock}>
          <p className={styles.greyText}>Usable</p>
          <p className={styles.boldText}> 24 GB</p>
        </div>
      </div>
    </div>
    <div className={styles.infoColumn}>
      <p className={styles.columnName}>Replicasets</p>
      <div className={styles.centredWrapper}>
        <p className={styles.boldText}>12</p>
        <p className={styles.noMargin}>total replicasets</p>
      </div>
    </div>
    <div className={styles.infoColumn}>
      <p className={styles.columnName}>Instances</p>
      <div className={styles.instancesBlock}>
        <div className={styles.centredWrapper}>
          <p className={styles.boldText}>12</p>
          <p className={styles.noMargin}>current grade online</p>
        </div>
        <div className={styles.centredWrapper}>
          <p className={styles.boldText}>12</p>
          <p className={styles.noMargin}>current grade offline</p>
        </div>
      </div>
    </div>
    <div className={styles.infoColumn}>
      <p className={styles.columnName}>Version</p>
      <div className={styles.centredWrapper}>
        <p className={styles.boldText}>1.22</p>
        <p className={styles.noMargin}>current instance</p>
      </div>
    </div>
  </div>
);
