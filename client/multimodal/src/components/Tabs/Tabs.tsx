import { Tabs as RadixTabs, Box } from "@radix-ui/themes";

import styles from "./tabs.module.css";

export type TabModel = {
  type: string;
  title: string;
  content: () => JSX.Element;
};

export interface TabsProps {
  tabs: TabModel[];
  onChangeTab?: (tab: string) => void;
}

export function Tabs(props: TabsProps) {
  const handleChangeTab = (value: string) => {
    props.onChangeTab?.(value);
  };

  return (
    <RadixTabs.Root
      defaultValue={props.tabs[0]?.type}
      className={styles.root}
      onValueChange={handleChangeTab}
    >
      <RadixTabs.List>
        {props.tabs.map((tab) => {
          return (
            <RadixTabs.Trigger value={tab.type}>{tab.title}</RadixTabs.Trigger>
          );
        })}
      </RadixTabs.List>

      <Box className={styles.content}>
        {props.tabs.map((tab) => {
          return (
            <RadixTabs.Content value={tab.type}>
              {tab.content()}
            </RadixTabs.Content>
          );
        })}
      </Box>
    </RadixTabs.Root>
  );
}
