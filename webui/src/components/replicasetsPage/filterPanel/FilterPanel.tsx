import styles from "./FilterPanel.module.css";

export const FilterPanel = () => {
  return (
    <div className={styles.wrapper}>
      <input className={styles.searchBar} />
      <div className={styles.buttonsWrapper}>
        <button className={styles.button}>Filter by</button>
        <button className={styles.button}>Sort by</button>
        <button className={styles.button}>Group by</button>
      </div>
    </div>
  );
};
