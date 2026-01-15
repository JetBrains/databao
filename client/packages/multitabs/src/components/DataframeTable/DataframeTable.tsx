import styles from "./dataframeTable.module.css";

interface DataframeTableProps {
  htmlContent: string;
}

export function DataframeTable(props: DataframeTableProps) {
  return (
    <div
      className={styles.root}
      dangerouslySetInnerHTML={{ __html: props.htmlContent }}
    />
  );
}
