type Truthy<T> = T extends false | "" | 0 | null | undefined ? never : T;

export function truthy<T>(value: T): value is Truthy<T> {
  return !!value;
}

export type Override<T, U> = Omit<T, keyof T & keyof U> & U;
