export type Override<T, U> = Omit<T, keyof T & keyof U> & U;
