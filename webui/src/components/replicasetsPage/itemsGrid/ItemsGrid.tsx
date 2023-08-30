import styles from "./ItemsGrid.module.css";
import { FilterPanel } from "../filterPanel/FilterPanel";
import { ReplicasetCard } from "../replicasetCard/ReplicasetCard";

const replicasets = [
  {
    id: "Test123",
    instanceCount: 2,
    instances: [
      {
        name: "Test 1",
        targetGrade: "target grade",
        currentGrade: "current grade",
        failureDomain: "Failure",
        version: "1.2",
        isLeader: true,
      },
      {
        name: "Test 2",
        targetGrade: "target grade",
        currentGrade: "current grade",
        failureDomain: "Failure",
        version: "1.2",
        isLeader: false,
      },
    ],
    version: "1.1",
    grade: "Main grade",
    capacity: "2,5MiB/50 GiB",
  },
  {
    id: "Test0",
    instanceCount: 3,
    instances: [
      {
        name: "Best",
        targetGrade: "target grade",
        currentGrade: "current grade",
        failureDomain: "Failure",
        version: "1.2",
        isLeader: false,
      },
      {
        name: "Lovely",
        targetGrade: "target grade",
        currentGrade: "current grade",
        failureDomain: "Failure",
        version: "1.2",
        isLeader: true,
      },
    ],
    version: "1.1",
    grade: "Main grade",
    capacity: "2,5MiB/50 GiB",
  },
  {
    id: "Test123",
    instanceCount: 1,
    instances: [
      {
        name: "Donatello",
        targetGrade: "target grade",
        currentGrade: "current grade",
        failureDomain: "Failure",
        version: "1.2",
        isLeader: true,
      },
      {
        name: "Leonardo",
        targetGrade: "target grade",
        currentGrade: "current grade",
        failureDomain: "Failure",
        version: "1.2",
        isLeader: false,
      },
      {
        name: "Shredder",
        targetGrade: "target grade",
        currentGrade: "current grade",
        failureDomain: "Failure",
        version: "1.2",
        isLeader: false,
      },
    ],
    version: "1.1",
    grade: "Main grade",
    capacity: "2,5MiB/50 GiB",
  },
  {
    id: "Test123",
    instanceCount: 2,
    instances: [
      {
        name: "Raphael",
        targetGrade: "target grade",
        currentGrade: "current grade",
        failureDomain: "Failure",
        version: "1.2",
        isLeader: false,
      },
      {
        name: "Michelangelo",
        targetGrade: "target grade",
        currentGrade: "current grade",
        failureDomain: "Failure",
        version: "1.2",
        isLeader: true,
      },
    ],
    version: "1.1",
    grade: "Main grade",
    capacity: "2,5MiB/50 GiB",
  },
];

export const ItemsGrid = ({}) => {
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
