import { grey } from "@mui/material/colors";

type StreamArrowProps = {
  direction?: "up" | "down";
  inactive?: boolean;
};
export const StreamArrow = ({
  direction = "down",
  inactive = false,
}: StreamArrowProps) => {
  return (
    <svg
      style={{
        transform: direction === "up" ? "rotate(180deg)" : undefined,
      }}
      width="24"
      height="100%"
      viewBox="0 0 24 100"
      preserveAspectRatio="none"
      fill="none"
    >
      <line
        x1="12"
        y1="6"
        x2="12"
        y2="88"
        stroke={grey[500]}
        strokeWidth="2"
        strokeLinecap="round"
        strokeDasharray={inactive ? "5 5" : undefined}
      />
      <path
        d="M6 82 L12 94 L18 82"
        stroke={grey[500]}
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
};
