import { useCallback, useSyncExternalStore } from "react";
import { z } from "zod";

export interface LsStateOptions<T extends z.ZodSchema> {
  key: string;
  schema: T;
  defaultValue: z.infer<T>;
}

// All hook instances using the same key must observe the same value,
// otherwise a write from one component (e.g. login) is invisible to
// components mounted earlier (e.g. the session refresher).
const cache = new Map<string, unknown>();
const listeners = new Map<string, Set<() => void>>();

function subscribe(key: string, listener: () => void) {
  let keyListeners = listeners.get(key);
  if (!keyListeners) {
    keyListeners = new Set();
    listeners.set(key, keyListeners);
  }
  keyListeners.add(listener);

  return () => {
    keyListeners?.delete(listener);
  };
}

function readCached<T extends z.ZodSchema>(args: LsStateOptions<T>) {
  if (!cache.has(args.key)) {
    let value: z.infer<T>;
    try {
      value = getLsValue<T>(args);
    } catch (e) {
      value = args.defaultValue;
    }
    cache.set(args.key, value);
  }

  return cache.get(args.key) as z.infer<T>;
}

export const useLsState = <T extends z.ZodSchema>(args: LsStateOptions<T>) => {
  const { key } = args;

  const value = useSyncExternalStore(
    useCallback((listener) => subscribe(key, listener), [key]),
    () => readCached(args)
  );

  const onChange = useCallback(
    (newValue?: z.infer<T>) => {
      cache.set(key, newValue);
      localStorage.setItem(key, JSON.stringify(newValue));
      listeners.get(key)?.forEach((listener) => listener());
    },
    [key]
  );

  return [value, onChange] as const;
};

export function getLsValue<T extends z.ZodSchema>(args: LsStateOptions<T>) {
  const lsValue = localStorage.getItem(args.key);
  const parsedValue = lsValue ? JSON.parse(lsValue) : args.defaultValue;
  const validatedValue = args.schema.parse(parsedValue);

  return validatedValue as z.infer<T>;
}
