import { Tabs, Box, Text } from "@radix-ui/themes";
import { VegaChart } from "@/components/VegaChart/VegaChart";
import { DataframeTable } from "@/components/DataframeTable/DataframeTable";
import styles from "./multimodalTabs.module.css";

interface MultimodalTabsProps {
  data: {
    spec: object;
    dataframeHtmlContent: string;
    text: string;
  };
}

export function MultimodalTabs({ data }: MultimodalTabsProps) {
  return (
    <Tabs.Root defaultValue="chart" className={styles.root}>
      <Tabs.List>
        <Tabs.Trigger value="chart">Chart</Tabs.Trigger>
        <Tabs.Trigger value="data">Data</Tabs.Trigger>
        <Tabs.Trigger value="text">Text</Tabs.Trigger>
      </Tabs.List>

      <Box className={styles.content}>
        <Tabs.Content value="chart">
          <VegaChart spec={data.spec} />
        </Tabs.Content>

        <Tabs.Content value="data">
          <DataframeTable htmlContent={data.dataframeHtmlContent} />
        </Tabs.Content>

        <Tabs.Content value="text">
          <Text color="gray">{data.text}</Text>
        </Tabs.Content>
      </Box>
    </Tabs.Root>
  );
}
