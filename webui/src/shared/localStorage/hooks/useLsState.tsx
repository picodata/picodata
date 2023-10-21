import { useCallback, useState } from "react";
import { z } from "zod";

export const useLsState = <T extends z.ZodSchema>(args: {
  key: string;
  schema: T;
  defaultValue?: z.infer<T>;
}) => {
  const [value, setValue] = useState(() => {
    try {
      const lsValue = localStorage.getItem(args.key);
      const parsedValue = lsValue ? JSON.parse(lsValue) : args.defaultValue;
      const validatedValue = args.schema.parse(parsedValue);

      return validatedValue as z.infer<T>;
    } catch (e) {
      return args.defaultValue;
    }
  });

  const onChange = useCallback(
    (newValue?: z.infer<T>) => {
      setValue(newValue);
      localStorage.setItem(args.key, JSON.stringify(newValue));
    },
    [args.key]
  );

  return [value, onChange] as const;
};
