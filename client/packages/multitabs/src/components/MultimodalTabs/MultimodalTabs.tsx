import { Tabs, Box } from "@radix-ui/themes";

import styles from "./multimodalTabs.module.css";

export type MultimodalTab = {
  type: string;
  title: string;
  content: () => JSX.Element;
};

export interface MultimodalTabsProps {
  tabs: MultimodalTab[];
  onChangeTab?: (tab: string) => void;
}

export function MultimodalTabs(props: MultimodalTabsProps) {
  const handleChangeTab = (value: string) => {
    props.onChangeTab?.(value);
  };

  return (
    <Tabs.Root
      defaultValue="CHART"
      className={styles.root}
      onValueChange={handleChangeTab}
    >
      <Tabs.List>
        {props.tabs.map((tab) => {
          return <Tabs.Trigger value={tab.type}>{tab.title}</Tabs.Trigger>;
        })}
      </Tabs.List>

      <Box className={styles.content}>
        {props.tabs.map((tab) => {
          return <Tabs.Content value={tab.type}>{tab.content()}</Tabs.Content>;
        })}
      </Box>
    </Tabs.Root>
  );
}
