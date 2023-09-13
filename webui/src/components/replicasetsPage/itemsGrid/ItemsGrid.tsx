import styles from "./ItemsGrid.module.css";
import { FilterPanel } from "../filterPanel/FilterPanel";
import { ReplicasetCard } from "../replicasetCard/ReplicasetCard";
import { useDispatch, useSelector } from "react-redux";
import { useEffect } from "react";
import { getReplicasets } from "store/slices/clusterSlice";
import { AppDispatch, RootState } from "store";

export const ItemsGrid = ({}) => {
  const dispatch = useDispatch<AppDispatch>();
  const replicasets = useSelector(
    (state: RootState) => state.cluster.replicasets
  );
  useEffect(() => {
    dispatch(getReplicasets());
  }, [dispatch]);
  return (
    <div className={styles.gridWrapper}>
      <FilterPanel />
      <div className={styles.replicasetsWrapper}>
        {replicasets.map((rep) => (
          <ReplicasetCard key={rep.id} replicaset={rep} />
        ))}
      </div>
    </div>
  );
};
