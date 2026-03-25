import { Tabs as RadixTabs, Box } from "@radix-ui/themes";
import { useCallback, useState } from "react";

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
  const { onChangeTab } = props;
  const [activeTab, setActiveTab] = useState(props.tabs[0]?.type);

  const handleChangeTab = useCallback(
    (value: string) => {
      setActiveTab(value);
      onChangeTab?.(value);
    },
    [onChangeTab],
  );

  return (
    <RadixTabs.Root
      value={activeTab}
      className={styles.root}
      onValueChange={handleChangeTab}
    >
      <RadixTabs.List>
        {props.tabs.map((tab) => {
          return (
            <RadixTabs.Trigger key={tab.type} value={tab.type}>
              {tab.title}
            </RadixTabs.Trigger>
          );
        })}
      </RadixTabs.List>

      <Box className={styles.content}>
        {props.tabs.map((tab) => {
          return (
            <RadixTabs.Content key={tab.type} value={tab.type} forceMount>
              <div
                style={{ display: activeTab === tab.type ? "block" : "none" }}
              >
                {tab.content()}
              </div>
            </RadixTabs.Content>
          );
        })}
      </Box>
    </RadixTabs.Root>
  );
}
