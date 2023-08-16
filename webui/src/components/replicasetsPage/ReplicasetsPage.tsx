import { FilterPanel } from "./filterPanel/FilterPanel";
import { ReplicasetCard } from "./replicasetCard/ReplicasetCard";
import styles from "./ReplicasetsPage.module.css";

const replicasets = [
  {
    id: "Test123",
    roles: ["test1", "test2", "test3"],
    isProblem: false,
    status: "Good",
    instances: [
      {
        name: "Instance Test 1",
        info: "Some info",
      },
      {
        name: "Instance Test 2",
        info: "Some info",
      },
    ],
    errorsCount: 0,
    version: "1.1",
  },
  {
    id: "Test0",
    roles: ["test0", "qwe", "asd"],
    isProblem: false,
    status: "Good",
    instances: [
      {
        name: "ASDASD",
        info: "ASDSADASDASDASD",
      },
      {
        name: "ASDASASDASDASD",
        info: "QWEWQEWEQWEQWE",
      },
    ],
    errorsCount: 0,
    version: "1.1",
  },
  {
    id: "Test123",
    roles: ["test1", "test2", "test3"],
    isProblem: true,
    status: "Bad",
    instances: [
      {
        name: "Instance Test 1",
        info: "Some info",
      },
      {
        name: "Instance Test 2",
        info: "Some info",
      },
    ],
    errorsCount: 0,
    version: "1.1",
  },
  {
    id: "Test123",
    roles: ["test1", "test2", "test3"],
    isProblem: true,
    status: "Bad",
    instances: [
      {
        name: "Instance Test 1",
        info: "Some info",
      },
      {
        name: "Instance Test 2",
        info: "Some info",
      },
    ],
    errorsCount: 0,
    version: "1.1",
  },
];

export const ReplicasetsPage = () => {
  return (
    <div className={styles.pageWrapper}>
      <FilterPanel />
      <div className={styles.replicasetsWrapper}>
        {replicasets.map((rep) => (
          <ReplicasetCard key={rep.id} replicaset={rep} />
        ))}
      </div>
    </div>
  );
};
