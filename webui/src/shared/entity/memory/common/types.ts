export type Memory = {
  usable: number;
  used: number;
};

export type MemoryItem = Memory & {
  name: string;
};

export type ReplicasetMemory = MemoryItem;

export type TierMemory = MemoryItem & {
  replicasets: ReplicasetMemory[];
};

export type ServerMemoryType = TierMemory[];
