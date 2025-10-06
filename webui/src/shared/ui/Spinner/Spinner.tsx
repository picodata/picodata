import { FC } from "react";

import { SpinnerProps } from "./Spinner.types";

import styles from "./Spinner.module.scss";

export const Spinner: FC<SpinnerProps> = ({ size = 14 }) => (
  <svg
    className={styles.spinner}
    aria-hidden="true"
    width={`${size}px`}
    height={`${size}px`}
    viewBox="0 0 14 14"
    fill="none"
    xmlns="http://www.w3.org/2000/svg"
  >
    <path
      d="M12.25 7.00001C12.2499 8.10869 11.8989 9.18889 11.2472 10.0858C10.5955 10.9827 9.6766 11.6503 8.62218 11.9929C7.56775 12.3354 6.43195 12.3354 5.37754 11.9928C4.32314 11.6501 3.40426 10.9825 2.75261 10.0856C2.10096 9.18861 1.74999 8.10839 1.75 6.99972C1.75001 5.89104 2.10099 4.81083 2.75266 3.91389C3.40432 3.01695 4.3232 2.34933 5.37761 2.00672C6.43202 1.66411 7.56783 1.6641 8.62225 2.00668"
      stroke="#F0F0F0"
      strokeWidth="1.3"
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  </svg>
);
